# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 6 — Verify and Cleanup
# MAGIC
# MAGIC **Purpose:** Final reconciliation and cleanup after the S3 migration and database
# MAGIC updates.  This notebook:
# MAGIC
# MAGIC 1. Verifies every `tblS3Image.s3FilePath` record has a corresponding S3 object.
# MAGIC 2. Confirms no references to old county folder names remain in `tblS3Image`.
# MAGIC 3. Verifies no objects remain under old county folder prefixes in S3.
# MAGIC 4. Produces a before/after comparison report.
# MAGIC
# MAGIC **DRY_RUN:** This notebook is read-only (no destructive operations) and is safe
# MAGIC to run at any time.
# MAGIC
# MAGIC **Pre-requisite:** Notebooks 0–5 have completed successfully.

# COMMAND ----------

# %run ./0_setup_and_config   # ← uncomment in Databricks

import os
import sys
import logging
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1] if "__file__" in dir() else Path("/Workspace/Repos/LND-7726")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.database_utils import DatabaseConnection
from utils.s3_utils import S3Client, list_county_objects, get_s3_key_from_path
from utils.validation_utils import reconcile_paths

logger = logging.getLogger("LND-7726.verify")


def _get_widget_or_env(widget_name: str, env_var: str, default: str) -> str:
    """Retrieve a value from a Databricks widget, env var, or hard-coded default."""
    try:
        dbutils.widgets.text(widget_name, os.environ.get(env_var, default))  # noqa: F821
        return dbutils.widgets.get(widget_name)  # noqa: F821
    except NameError:
        return os.environ.get(env_var, default)


# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------
try:
    _ = DATABASES  # noqa: F821
    _ = s3_client  # noqa: F821
except NameError:
    DRY_RUN      = os.environ.get("DRY_RUN", "true").lower() in ("1", "true", "yes")
    DB_NAME_1    = os.environ.get("DB_NAME_1", "database_name_1")
    DB_NAME_2    = os.environ.get("DB_NAME_2", "database_name_2")
    DB_SERVER    = os.environ.get("DB_SERVER", "")
    DB_USERNAME  = os.environ.get("DB_USERNAME", "")
    DB_PASSWORD  = os.environ.get("DB_PASSWORD", "")
    S3_BUCKET    = os.environ.get("S3_BUCKET", "enverus-courthouse-prod-chd-plants")
    STATE_PREFIX = os.environ.get("STATE_PREFIX", "tx")
    DATABASES    = {
        DB_NAME_1: DatabaseConnection(DB_NAME_1, DB_SERVER, DB_USERNAME, DB_PASSWORD, DRY_RUN),
        DB_NAME_2: DatabaseConnection(DB_NAME_2, DB_SERVER, DB_USERNAME, DB_PASSWORD, DRY_RUN),
    }
    for _conn in DATABASES.values():
        _conn.connect()
    s3_client = S3Client(bucket=S3_BUCKET, region=os.environ.get("AWS_REGION", "us-east-1"))

MIGRATION_MAP_PATH: str = _get_widget_or_env(
    "migration_map_path", "MIGRATION_MAP_PATH", ""
)

# COMMAND ----------
# MAGIC %md ## 0. Load migration map from parquet or Delta table

# COMMAND ----------


def load_migration_map() -> list[dict]:
    """
    Load the migration map in priority order:

    1. Delta table ``county_folder_migration_map`` (written by Notebook 2,
       available when running inside Databricks with Spark).
    2. Parquet file at ``MIGRATION_MAP_PATH`` (set via the
       ``migration_map_path`` widget or ``MIGRATION_MAP_PATH`` env var).

    Raises
    ------
    RuntimeError
        If neither source is reachable.
    """
    # 1 — Delta table (Databricks / Spark)
    try:
        df = spark.table("county_folder_migration_map")  # noqa: F821
        rows = [row.asDict() for row in df.collect()]
        logger.info("Migration map loaded from Delta table: %d row(s)", len(rows))
        return rows
    except Exception as exc:
        logger.info("Delta table not available (%s) — trying parquet file.", exc)

    # 2 — Parquet file
    if not MIGRATION_MAP_PATH:
        raise RuntimeError(
            "No migration map source found. "
            "Set the 'migration_map_path' widget (or MIGRATION_MAP_PATH env var) "
            "to a parquet file path, or ensure the Delta table "
            "'county_folder_migration_map' is available."
        )

    import pandas as pd  # noqa: PLC0415

    logger.info("Loading migration map from parquet: %s", MIGRATION_MAP_PATH)
    df_pq = pd.read_parquet(MIGRATION_MAP_PATH)

    required_cols = {"database_name", "county_id", "county_name", "old_county_folder", "new_county_folder"}
    missing = required_cols - set(df_pq.columns)
    if missing:
        raise ValueError(
            f"Parquet file is missing required column(s): {sorted(missing)}. "
            f"Expected: {sorted(required_cols)}"
        )

    rows = df_pq[sorted(required_cols)].to_dict(orient="records")
    logger.info("Migration map loaded from parquet: %d row(s)", len(rows))
    return rows


MIGRATION_MAP = load_migration_map()
logger.info("Migration map: %d total path entries", len(MIGRATION_MAP))

# COMMAND ----------
# MAGIC %md ## 1. Verify every tblS3Image record has a matching S3 object

# COMMAND ----------

S3IMAGE_ALL_SQL = "SELECT recordID, s3FilePath FROM tblS3Image"

verification_results: dict[str, dict] = {}

for db_name, conn in DATABASES.items():
    rows = conn.execute_query(S3IMAGE_ALL_SQL)
    logger.info("[%s] Fetched %d tblS3Image row(s) for verification", db_name, len(rows))

    # Collect all S3 object keys present in the bucket for this state
    all_s3_objects_response = s3_client.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=f"{STATE_PREFIX}/",
    )
    s3_keys_present = {obj["Key"] for obj in all_s3_objects_response.get("Contents", [])}

    result = reconcile_paths(rows, s3_keys_present, S3_BUCKET)
    verification_results[db_name] = result

# COMMAND ----------
# MAGIC %md ## 2. Check for stale references to old county folder names in tblS3Image

# COMMAND ----------

# Derive old folder names from the migration map
old_folders = list({entry["old_county_folder"] for entry in MIGRATION_MAP})

STALE_REFS_SQL_TEMPLATE = (
    "SELECT recordID, s3FilePath "
    "FROM tblS3Image "
    "WHERE s3FilePath LIKE '%/{old_folder}/%'"
)

stale_by_db: dict[str, list[dict]] = {}

for db_name, conn in DATABASES.items():
    stale_rows: list[dict] = []
    for old_folder in old_folders:
        sql  = STALE_REFS_SQL_TEMPLATE.format(old_folder=old_folder)
        rows = conn.execute_query(sql)
        stale_rows.extend(rows)
    stale_by_db[db_name] = stale_rows
    if stale_rows:
        logger.warning("[%s] %d stale path(s) still reference old county folder(s).",
                       db_name, len(stale_rows))

# COMMAND ----------
# MAGIC %md ## 3. Check for remaining objects in old S3 folders

# COMMAND ----------

old_folder_remnants: dict[str, list] = {}

for old_folder in old_folders:
    objects = list_county_objects(s3_client, STATE_PREFIX, old_folder)
    old_folder_remnants[old_folder] = objects
    if objects:
        logger.warning("Old S3 folder '%s/%s/' still contains %d object(s).",
                       STATE_PREFIX, old_folder, len(objects))

# COMMAND ----------
# MAGIC %md ## 4. Reconciliation report

# COMMAND ----------

print("\n" + "=" * 80)
print("  VERIFICATION REPORT")
print("=" * 80)

# 4a — S3 object existence per database
print("\n  [A] tblS3Image → S3 object existence\n")
print(f"  {'Database':<22} {'Total Records':>14} {'Present ✅':>12} {'Missing ❌':>12}")
print("  " + "-" * 64)

grand_total   = 0
grand_present = 0
grand_missing = 0

for db_name, result in verification_results.items():
    total   = len(result["matched"]) + len(result["missing"])
    present = len(result["matched"])
    missing = len(result["missing"])
    grand_total   += total
    grand_present += present
    grand_missing += missing
    print(f"  {db_name:<22} {total:>14} {present:>12} {missing:>12}")

print("  " + "-" * 64)
print(f"  {'TOTAL':<22} {grand_total:>14} {grand_present:>12} {grand_missing:>12}")

# 4b — Stale path references
print("\n  [B] Stale references to old county folder names in tblS3Image\n")
any_stale = False
for db_name, stale_rows in stale_by_db.items():
    if stale_rows:
        any_stale = True
        print(f"  ⚠️  [{db_name}] {len(stale_rows)} stale path(s):")
        for row in stale_rows[:5]:
            print(f"       recordID={row.get('recordID')}  path={row.get('s3FilePath')}")
        if len(stale_rows) > 5:
            print(f"       … and {len(stale_rows) - 5} more")
    else:
        print(f"  ✅  [{db_name}] No stale path references found.")

# 4c — Remaining old S3 folders
print("\n  [C] Remaining objects in old S3 county folders\n")
any_remnants = False
for old_folder, objects in old_folder_remnants.items():
    if objects:
        any_remnants = True
        print(f"  ⚠️  '{STATE_PREFIX}/{old_folder}/' still has {len(objects)} object(s).")
    else:
        print(f"  ✅  '{STATE_PREFIX}/{old_folder}/' is empty.")

# COMMAND ----------
# MAGIC %md ## 5. Final determination

# COMMAND ----------

all_clear = (grand_missing == 0) and (not any_stale) and (not any_remnants)

print("\n" + "=" * 80)
if all_clear:
    print("  ✅  ALL CHECKS PASSED — Migration is complete and verified.")
else:
    print("  🚨  ISSUES FOUND — review the report above before closing this task.")
    if grand_missing > 0:
        print(f"       • {grand_missing} tblS3Image record(s) have no S3 object.")
    if any_stale:
        print("       • Stale path references remain in tblS3Image.")
    if any_remnants:
        print("       • Old county folders still contain objects in S3.")
print("=" * 80)

if DRY_RUN:
    print("\n  ℹ️  Note: Running in DRY RUN mode.\n")

# COMMAND ----------
# MAGIC %md ## 6. Persist verification report

# COMMAND ----------

# Flatten verification results into a single list for Delta persistence
verification_rows: list[dict] = []
for db_name, result in verification_results.items():
    for row in result["matched"]:
        verification_rows.append({**row, "database_name": db_name, "s3_exists": True})
    for row in result["missing"]:
        verification_rows.append({**row, "database_name": db_name, "s3_exists": False})

VERIFICATION_TABLE = "county_migration_verification"

try:
    if spark and verification_rows:  # noqa: F821
        df_ver = spark.createDataFrame(verification_rows)  # noqa: F821
        (
            df_ver.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(VERIFICATION_TABLE)
        )
        logger.info("Verification report written to '%s' (%d rows)", VERIFICATION_TABLE, len(verification_rows))
    else:
        logger.info("Verification report held in memory (%d rows).", len(verification_rows))
except Exception as exc:
    logger.warning("Could not persist verification report: %s", exc)

# Expose for downstream use / ad-hoc investigation
VERIFICATION_RESULTS  = verification_results
STALE_REFS            = stale_by_db
OLD_FOLDER_REMNANTS   = old_folder_remnants
