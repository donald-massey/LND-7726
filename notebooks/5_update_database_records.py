# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 5 — Update Database Records
# MAGIC
# MAGIC **Purpose:** Update `tblS3Image.s3FilePath` in both databases so every path
# MAGIC reflects the new county folder name.
# MAGIC
# MAGIC **Safety:**
# MAGIC   - DRY_RUN mode (default `True`) runs `SELECT COUNT(*)` instead of `UPDATE`.
# MAGIC   - Updates are scoped by `countyID` to prevent cross-county changes.
# MAGIC   - Every update is logged with rows-affected counts.
# MAGIC   - The notebook reads the `county_folder_migration_map` Delta table, making
# MAGIC     it independent of the in-session state.
# MAGIC
# MAGIC **Pre-requisite:** Notebook 4 completed with zero S3 errors.

# COMMAND ----------

# %run ./0_setup_and_config   # ← uncomment in Databricks

import sys
import logging
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1] if "__file__" in dir() else Path("/Workspace/Repos/LND-7726")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.database_utils import (
    MockDatabaseConnection,
    batch_update_paths,
    build_update_path_query,
    build_count_query,
)

logger = logging.getLogger("LND-7726.update_db")

# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------
try:
    _ = DATABASES  # noqa: F821
    _ = MIGRATION_MAP  # noqa: F821
except NameError:
    DRY_RUN   = True
    DB_NAME_1 = "database_name_1"
    DB_NAME_2 = "database_name_2"
    DB_SERVER = "mock-server"
    BATCH_SIZE = 100
    DATABASES = {
        DB_NAME_1: MockDatabaseConnection(DB_NAME_1, DB_SERVER, DRY_RUN),
        DB_NAME_2: MockDatabaseConnection(DB_NAME_2, DB_SERVER, DRY_RUN),
    }
    MIGRATION_MAP = [
        {
            "database_name":     DB_NAME_1,
            "county_id":         1,
            "county_name":       "CROCKETT2",
            "old_county_folder": "crockett2",
            "new_county_folder": "crockett",
        },
        {
            "database_name":     DB_NAME_1,
            "county_id":         2,
            "county_name":       "BEXAR1",
            "old_county_folder": "bexar1",
            "new_county_folder": "bexar",
        },
        {
            "database_name":     DB_NAME_2,
            "county_id":         3,
            "county_name":       "HARRIS3",
            "old_county_folder": "harris3",
            "new_county_folder": "harris",
        },
    ]

# COMMAND ----------
# MAGIC %md ## 1. Load migration map (prefer Delta table, fall back to in-memory)

# COMMAND ----------

def load_migration_map() -> list[dict]:
    """
    Attempt to load the persisted migration map from the Delta table written
    by Notebook 1.  Falls back to the in-memory `MIGRATION_MAP` list.
    """
    try:
        df = spark.table("county_folder_migration_map")  # noqa: F821
        rows = [row.asDict() for row in df.collect()]
        logger.info("Migration map loaded from Delta table: %d row(s)", len(rows))
        return rows
    except Exception as exc:
        logger.info("Delta table not available (%s) — using in-memory migration map.", exc)
        return MIGRATION_MAP


migration_map = load_migration_map()
logger.info("Migration map: %d total path entries", len(migration_map))

# COMMAND ----------
# MAGIC %md ## 2. Deduplicate map to unique (database, old_folder, new_folder, county_id) tuples

# COMMAND ----------

unique_updates: dict[tuple, dict] = {}

for entry in migration_map:
    key = (
        entry["database_name"],
        entry["old_county_folder"],
        entry["new_county_folder"],
        entry["county_id"],
    )
    unique_updates[key] = {
        "database_name":     entry["database_name"],
        "county_id":         entry["county_id"],
        "county_name":       entry["county_name"],
        "old_county_folder": entry["old_county_folder"],
        "new_county_folder": entry["new_county_folder"],
    }

logger.info("Unique DB update operations: %d", len(unique_updates))

# COMMAND ----------
# MAGIC %md ## 3. Apply updates to each database

# COMMAND ----------

update_log: list[dict] = []


def apply_db_updates(
    databases: dict,
    updates: list[dict],
    dry_run: bool,
    batch_size: int = 100,
) -> list[dict]:
    """
    Run path updates for every entry in *updates* against the appropriate
    database connection.

    Parameters
    ----------
    databases  : dict mapping db_name → connection
    updates    : list of dicts with database_name, county_id, old_county_folder, new_county_folder
    dry_run    : if True, run COUNT queries instead of UPDATE
    batch_size : passed through to batch_update_paths

    Returns
    -------
    List of log dicts (one per update).
    """
    log: list[dict] = []

    # Group by database
    by_db: dict[str, list[dict]] = {}
    for entry in updates:
        by_db.setdefault(entry["database_name"], []).append(entry)

    for db_name, db_updates in by_db.items():
        conn = databases.get(db_name)
        if conn is None:
            logger.error("No connection for database '%s' — skipping %d update(s).", db_name, len(db_updates))
            continue

        logger.info("[%s] Applying %d update(s)…", db_name, len(db_updates))
        results = batch_update_paths(conn, db_updates, batch_size=batch_size)

        for entry in db_updates:
            cid         = str(entry["county_id"])
            rows_aff    = results.get(cid, -1)
            log_entry = {
                "database_name":     db_name,
                "county_id":         entry["county_id"],
                "county_name":       entry["county_name"],
                "old_county_folder": entry["old_county_folder"],
                "new_county_folder": entry["new_county_folder"],
                "rows_affected":     rows_aff,
                "dry_run":           dry_run,
                "status":            "ok" if rows_aff >= 0 else "error",
            }
            log.append(log_entry)

    return log


update_log = apply_db_updates(
    DATABASES,
    list(unique_updates.values()),
    DRY_RUN,
    BATCH_SIZE,
)

# COMMAND ----------
# MAGIC %md ## 4. Verify update counts match expected record counts

# COMMAND ----------

print("\n🔍  Verifying expected vs. actual row counts…\n")

# Count expected records per (db, county_id)
expected_counts: dict[tuple, int] = {}
for entry in migration_map:
    key = (entry["database_name"], entry["county_id"])
    expected_counts[key] = expected_counts.get(key, 0) + 1

for log_entry in update_log:
    key      = (log_entry["database_name"], log_entry["county_id"])
    expected = expected_counts.get(key, 0)
    actual   = log_entry["rows_affected"]
    # In DRY_RUN mode the DB returns 0; mark as ✅ only if expected also 0 or DRY_RUN
    match    = "✅" if (actual == expected or (DRY_RUN and actual == 0)) else "❌"
    print(
        f"  [{log_entry['database_name']}] "
        f"{log_entry['county_name']:<15} "
        f"expected={expected:>4}  affected={actual:>4}  {match}"
    )

# COMMAND ----------
# MAGIC %md ## 5. Persist update log

# COMMAND ----------

DB_UPDATE_TABLE = "county_db_update_log"

try:
    if spark and update_log:  # noqa: F821
        df_log = spark.createDataFrame(update_log)  # noqa: F821
        (
            df_log.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(DB_UPDATE_TABLE)
        )
        logger.info("Update log written to '%s' (%d rows)", DB_UPDATE_TABLE, len(update_log))
    else:
        logger.info("Update log held in memory (%d rows).", len(update_log))
except Exception as exc:
    logger.warning("Could not persist update log: %s", exc)

# COMMAND ----------
# MAGIC %md ## 6. Summary report

# COMMAND ----------

total_rows    = sum(e.get("rows_affected", 0) for e in update_log)
error_entries = [e for e in update_log if e["status"] == "error"]

print("\n" + "=" * 70)
print("  DATABASE UPDATE SUMMARY")
print("=" * 70)
print(f"  Databases updated : {len(set(e['database_name'] for e in update_log))}")
print(f"  Counties updated  : {len(update_log)}")
print(f"  Total rows updated: {total_rows}")
print(f"  Errors            : {len(error_entries)}")
print("=" * 70)

if DRY_RUN:
    print("\n  ⚠️  DRY RUN — no database records were modified.\n")
elif error_entries:
    print(f"\n  🚨  {len(error_entries)} error(s) occurred.  Review the update log.\n")
else:
    print("\n  ✅  All database records updated.  Proceed to Notebook 6 for verification.\n")

# Expose for downstream use
DB_UPDATE_LOG = update_log
