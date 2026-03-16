# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 3 — Pre-Migration Inventory
# MAGIC
# MAGIC **Purpose:** Before any objects are moved, create a detailed inventory that
# MAGIC compares every `tblS3Image` record against the actual objects in S3.  The
# MAGIC inventory documents:
# MAGIC   - Counties to migrate
# MAGIC   - Current (old) folder paths in S3
# MAGIC   - Target (new) folder paths
# MAGIC   - Object counts per county
# MAGIC   - Any discrepancies (records in DB with no S3 object, or vice-versa)
# MAGIC
# MAGIC **Pre-requisite:** Notebooks 0–2 have been run successfully.

# COMMAND ----------

# %run ./0_setup_and_config   # ← uncomment in Databricks
# %run ./1_discovery_and_analysis

import sys
import logging
from pathlib import Path
from collections import defaultdict

REPO_ROOT = Path(__file__).resolve().parents[1] if "__file__" in dir() else Path("/Workspace/Repos/LND-7726")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.database_utils import DatabaseConnection, SAMPLE_S3_IMAGE
from utils.s3_utils import MockS3Client, list_county_objects, get_s3_key_from_path
from utils.validation_utils import reconcile_paths

logger = logging.getLogger("LND-7726.inventory")

# ---------------------------------------------------------------------------
# Config defaults (overridden by setup / discovery notebooks when %run active)
# ---------------------------------------------------------------------------
try:
    _ = DATABASES  # noqa: F821
    _ = MIGRATION_MAP  # noqa: F821
    _ = s3_client  # noqa: F821
except NameError:
    DRY_RUN      = True
    DB_NAME_1    = "database_name_1"
    DB_NAME_2    = "database_name_2"
    DB_SERVER    = "mock-server"
    S3_BUCKET    = "enverus-courthouse-prod-chd-plants"
    STATE_PREFIX = "tx"
    DATABASES    = {
        DB_NAME_1: DatabaseConnection(DB_NAME_1, DB_SERVER, DRY_RUN),
        DB_NAME_2: DatabaseConnection(DB_NAME_2, DB_SERVER, DRY_RUN),
    }
    s3_client = MockS3Client(bucket=S3_BUCKET, dry_run=DRY_RUN)
    # Minimal migration map for standalone runs
    MIGRATION_MAP = [
        {
            "database_name":     DB_NAME_1,
            "county_id":         1,
            "county_name":       "CROCKETT2",
            "diml_county_name":  "CROCKETT_TX",
            "old_county_folder": "crockett2",
            "new_county_folder": "crockett",
            "record_id":         101,
            "old_s3_path":       "s3://enverus-courthouse-prod-chd-plants/tx/crockett2/e1ed/e1edc1e1-8608-4f03-88a0-72844609af94.pdf",
            "new_s3_path":       "s3://enverus-courthouse-prod-chd-plants/tx/crockett/e1ed/e1edc1e1-8608-4f03-88a0-72844609af94.pdf",
        },
    ]

# COMMAND ----------
# MAGIC %md ## 1. Collect distinct counties to migrate (from migration map)

# COMMAND ----------

# De-duplicate: one entry per (db_name, old_county_folder, new_county_folder)
county_specs: dict[tuple, dict] = {}
for entry in MIGRATION_MAP:
    key = (entry["database_name"], entry["old_county_folder"], entry["new_county_folder"])
    county_specs.setdefault(key, {
        "database_name":     entry["database_name"],
        "county_id":         entry["county_id"],
        "county_name":       entry["county_name"],
        "old_county_folder": entry["old_county_folder"],
        "new_county_folder": entry["new_county_folder"],
        "db_record_count":   0,
        "s3_object_count":   0,
        "discrepancy_count": 0,
    })
    county_specs[key]["db_record_count"] += 1

logger.info("Distinct counties to inventory: %d", len(county_specs))

# COMMAND ----------
# MAGIC %md ## 2. Query tblS3Image for each county and verify against S3

# COMMAND ----------

S3IMAGE_SQL_TEMPLATE = (
    "SELECT i.recordID, i.s3FilePath "
    "FROM tblS3Image i "
    "JOIN tblRecord r ON r.recordId = i.recordID "
    "WHERE r.countyID = {county_id}"
)

inventory_rows: list[dict] = []

for spec_key, spec in county_specs.items():
    db_name    = spec["database_name"]
    county_id  = spec["county_id"]
    old_folder = spec["old_county_folder"]
    new_folder = spec["new_county_folder"]
    conn       = DATABASES.get(db_name)

    if conn is None:
        logger.warning("No connection for database '%s' — skipping county %s", db_name, old_folder)
        continue

    # Fetch DB records for this county
    sql  = S3IMAGE_SQL_TEMPLATE.format(county_id=county_id)
    rows = conn.execute_query(sql)
    logger.info("[%s] County '%s': %d DB record(s)", db_name, old_folder, len(rows))

    # Fetch actual S3 objects under the old folder prefix
    s3_objects = list_county_objects(s3_client, STATE_PREFIX, old_folder)
    s3_keys    = {obj["Key"] for obj in s3_objects}

    # Reconcile DB records vs S3 objects
    reconcile_result = reconcile_paths(rows, s3_keys, S3_BUCKET)

    spec["db_record_count"]  = len(rows)
    spec["s3_object_count"]  = len(s3_objects)
    spec["discrepancy_count"] = len(reconcile_result["missing"])

    # Append per-file inventory rows
    for obj in s3_objects:
        inventory_rows.append({
            "database_name":     db_name,
            "county_name":       spec["county_name"],
            "old_county_folder": old_folder,
            "new_county_folder": new_folder,
            "s3_key":            obj["Key"],
            "size_bytes":        obj["Size"],
            "etag":              obj["ETag"],
            "in_database":       any(
                get_s3_key_from_path(r["s3FilePath"], S3_BUCKET) == obj["Key"]
                for r in rows
                if "s3FilePath" in r
            ),
        })

# COMMAND ----------
# MAGIC %md ## 3. Display county-level inventory summary

# COMMAND ----------

print("\n" + "=" * 90)
print("  PRE-MIGRATION INVENTORY — County Summary")
print("=" * 90)
print(f"  {'Database':<20} {'County':<15} {'Old Folder':<15} {'New Folder':<15} "
      f"{'DB Recs':>7} {'S3 Objs':>7} {'Issues':>7}")
print("  " + "-" * 88)

total_db   = 0
total_s3   = 0
total_disc = 0

for spec in county_specs.values():
    db_recs  = spec["db_record_count"]
    s3_objs  = spec["s3_object_count"]
    issues   = spec["discrepancy_count"]
    flag     = " ⚠️" if issues > 0 else " ✅"
    total_db   += db_recs
    total_s3   += s3_objs
    total_disc += issues
    print(
        f"  {spec['database_name']:<20} {spec['county_name']:<15} "
        f"{spec['old_county_folder']:<15} {spec['new_county_folder']:<15} "
        f"{db_recs:>7} {s3_objs:>7} {issues:>7}{flag}"
    )

print("  " + "-" * 88)
print(f"  {'TOTAL':<52} {total_db:>7} {total_s3:>7} {total_disc:>7}")
print("=" * 90)

# COMMAND ----------
# MAGIC %md ## 4. Persist inventory to Delta table

# COMMAND ----------

INVENTORY_TABLE = "county_migration_inventory"

try:
    if spark and inventory_rows:  # noqa: F821
        df_inv = spark.createDataFrame(inventory_rows)  # noqa: F821
        (
            df_inv.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(INVENTORY_TABLE)
        )
        logger.info("Inventory written to Delta table: %s (%d rows)", INVENTORY_TABLE, len(inventory_rows))
    else:
        logger.info("Inventory held in memory (%d rows) — Spark not available.", len(inventory_rows))
except Exception as exc:
    logger.warning("Could not write inventory Delta table: %s", exc)

# COMMAND ----------
# MAGIC %md ## 5. Summary

# COMMAND ----------

print("\n" + "=" * 70)
print("  INVENTORY SUMMARY")
print("=" * 70)
print(f"  Counties to migrate     : {len(county_specs)}")
print(f"  Total DB records        : {total_db}")
print(f"  Total S3 objects found  : {total_s3}")
print(f"  Total discrepancies     : {total_disc}")
print("=" * 70)

if total_disc > 0:
    print(f"\n  ⚠️  {total_disc} discrepancy(ies) found — review before proceeding.\n")
else:
    print("\n  ✅  No discrepancies found.  Safe to proceed with migration.\n")

# Expose for downstream notebooks
INVENTORY_ROWS = inventory_rows
COUNTY_SPECS   = county_specs
