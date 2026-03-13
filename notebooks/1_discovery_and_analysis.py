# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1 — Discovery and Analysis
# MAGIC
# MAGIC **Purpose:** Identify all county folders in S3 that have numeric suffixes or
# MAGIC special characters.  Prioritise by record count (descending) and produce a
# MAGIC migration mapping DataFrame ready for the downstream notebooks.
# MAGIC
# MAGIC **Pre-requisite:** Run `0_setup_and_config` first (or execute `%run ./0_setup_and_config`).

# COMMAND ----------

# %run ./0_setup_and_config   # ← uncomment in Databricks

# When running standalone (tests / local), import the shared config directly.
import sys
import os
import logging
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1] if "__file__" in dir() else Path("/Workspace/Repos/LND-7726")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.database_utils import (
    MockDatabaseConnection,
    build_discovery_query,
    SAMPLE_LOOKUP_COUNTIES,
)
from utils.validation_utils import extract_county_folder

logger = logging.getLogger("LND-7726.discovery")

# ---------------------------------------------------------------------------
# Config defaults (overridden by 0_setup_and_config when %run is active)
# ---------------------------------------------------------------------------
try:
    _ = DATABASES  # noqa: F821 — set by setup notebook
except NameError:
    from utils.database_utils import MockDatabaseConnection
    DRY_RUN      = True
    DB_NAME_1    = "database_name_1"
    DB_NAME_2    = "database_name_2"
    DB_SERVER    = "mock-server"
    STATE_PREFIX = "tx"
    DATABASES    = {
        DB_NAME_1: MockDatabaseConnection(DB_NAME_1, DB_SERVER, DRY_RUN),
        DB_NAME_2: MockDatabaseConnection(DB_NAME_2, DB_SERVER, DRY_RUN),
    }

# COMMAND ----------
# MAGIC %md ## 1. Run discovery query against both databases

# COMMAND ----------

def run_discovery(db_name: str, conn: MockDatabaseConnection, state_prefix: str) -> list[dict]:
    """
    Execute the discovery query and return rows where CountyName != S3Key
    (i.e. the folder in tblS3Image does not match the canonical S3Key).
    """
    sql = build_discovery_query(state_prefix)
    logger.info("Running discovery on '%s'", db_name)
    rows = conn.execute_query(sql)
    logger.info("  → %d mismatch row(s) found in '%s'", len(rows), db_name)
    return rows


all_discovery_rows: list[dict] = []

for db_name, conn in DATABASES.items():
    rows = run_discovery(db_name, conn, STATE_PREFIX)
    for r in rows:
        r["database_name"] = db_name        # tag each row with its source DB
    all_discovery_rows.extend(rows)

logger.info("Total mismatch rows across all databases: %d", len(all_discovery_rows))

# COMMAND ----------
# MAGIC %md ## 2. Build migration mapping

# COMMAND ----------

def build_migration_map(discovery_rows: list[dict]) -> list[dict]:
    """
    Derive the migration mapping from the discovery rows.

    Each mapping record contains:
      database_name | county_id | county_name | old_county_folder | new_county_folder
      | record_id | old_s3_path | new_s3_path
    """
    mapping: list[dict] = []

    for row in discovery_rows:
        s3_path        = row["s3FilePath"]
        old_folder     = extract_county_folder(s3_path)   # e.g. "crockett2"
        new_folder     = row["S3Key"].lower()              # e.g. "crockett"

        if old_folder is None:
            logger.warning("Could not extract county folder from: %s", s3_path)
            continue

        if old_folder == new_folder:
            # Path already uses the correct folder — no action needed.
            continue

        new_s3_path = s3_path.replace(f"/{old_folder}/", f"/{new_folder}/", 1)

        mapping.append({
            "database_name":     row["database_name"],
            "county_id":         row["CountyID"],
            "county_name":       row["CountyName"],
            "diml_county_name":  row["DIMLCountyName"],
            "old_county_folder": old_folder,
            "new_county_folder": new_folder,
            "record_id":         row["recordId"],
            "old_s3_path":       s3_path,
            "new_s3_path":       new_s3_path,
        })

    return mapping


migration_map = build_migration_map(all_discovery_rows)
logger.info("Migration map: %d path(s) to update", len(migration_map))

# COMMAND ----------
# MAGIC %md ## 3. Aggregate by county (priority order — largest first)

# COMMAND ----------

from collections import Counter

county_counts: Counter = Counter()
for entry in migration_map:
    key = (entry["database_name"], entry["county_name"], entry["old_county_folder"], entry["new_county_folder"])
    county_counts[key] += 1

# Sort descending by record count
priority_order = sorted(county_counts.items(), key=lambda x: x[1], reverse=True)

print("\n📋  Counties to migrate (priority order — largest first):\n")
print(f"  {'Database':<20} {'CountyName':<15} {'Old Folder':<15} {'New Folder':<15} {'Records':>8}")
print("  " + "-" * 78)
for (db_name, county_name, old_folder, new_folder), count in priority_order:
    print(f"  {db_name:<20} {county_name:<15} {old_folder:<15} {new_folder:<15} {count:>8}")

# COMMAND ----------
# MAGIC %md ## 4. Export migration map to Delta table (persisted for downstream notebooks)

# COMMAND ----------

MIGRATION_MAP_TABLE = "county_folder_migration_map"

try:
    # Convert to Spark DataFrame and write as Delta table
    if spark and migration_map:  # noqa: F821
        df_map = spark.createDataFrame(migration_map)  # noqa: F821
        (
            df_map.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(MIGRATION_MAP_TABLE)
        )
        logger.info("Migration map written to Delta table: %s", MIGRATION_MAP_TABLE)
    else:
        logger.info("Spark not available — migration map held in memory as `migration_map` list.")
except Exception as exc:
    logger.warning("Could not write Delta table (%s) — map held in memory.", exc)

# COMMAND ----------
# MAGIC %md ## 5. Summary report

# COMMAND ----------

print("\n" + "=" * 70)
print("  DISCOVERY SUMMARY")
print("=" * 70)
print(f"  Databases scanned    : {len(DATABASES)}")
print(f"  Total mismatch rows  : {len(all_discovery_rows)}")
print(f"  Unique counties      : {len(set(e['county_name'] for e in migration_map))}")
print(f"  Paths to update      : {len(migration_map)}")
print("=" * 70)
if DRY_RUN:
    print("\n  ⚠️  DRY RUN — no changes have been written.")
print()

# Expose the migration map for downstream notebooks run in the same session.
MIGRATION_MAP = migration_map
