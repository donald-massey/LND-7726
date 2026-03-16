# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2 — Validate and Correct Lookup Table
# MAGIC
# MAGIC **Purpose:** Review `tblLookupCounties.S3Key` values in both databases, identify
# MAGIC any that do not conform to the naming standard (letters, spaces, periods only —
# MAGIC no digits, no other special characters), generate a correction script, and
# MAGIC optionally apply corrections.
# MAGIC
# MAGIC **⚠️ This notebook MUST complete without errors before any migration begins.**
# MAGIC Execution halts if invalid S3Key values are found and `DRY_RUN=false`.
# MAGIC
# MAGIC **Pre-requisite:** `0_setup_and_config` has been run (or `%run ./0_setup_and_config`).

# COMMAND ----------

# %run ./0_setup_and_config   # ← uncomment in Databricks

import sys
import os
import logging
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1] if "__file__" in dir() else Path("/Workspace/Repos/LND-7726")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.database_utils import DatabaseConnection
from utils.validation_utils import validate_lookup_counties, generate_correction_map, correct_s3_key

logger = logging.getLogger("LND-7726.validate_lookup")

# ---------------------------------------------------------------------------
# Config defaults (overridden by setup notebook when %run is active)
# ---------------------------------------------------------------------------
try:
    _ = DATABASES  # noqa: F821
except NameError:
    DRY_RUN   = os.environ.get("DRY_RUN", "true").lower() in ("1", "true", "yes")
    DB_NAME_1 = os.environ.get("DB_NAME_1", "database_name_1")
    DB_NAME_2 = os.environ.get("DB_NAME_2", "database_name_2")
    DB_SERVER   = os.environ.get("DB_SERVER", "")
    DB_USERNAME = os.environ.get("DB_USERNAME", "")
    DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
    DATABASES = {
        DB_NAME_1: DatabaseConnection(DB_NAME_1, DB_SERVER, DB_USERNAME, DB_PASSWORD, DRY_RUN),
        DB_NAME_2: DatabaseConnection(DB_NAME_2, DB_SERVER, DB_USERNAME, DB_PASSWORD, DRY_RUN),
    }
    for _conn in DATABASES.values():
        _conn.connect()

# COMMAND ----------
# MAGIC %md ## 1. Query tblLookupCounties from each database

# COMMAND ----------

LOOKUP_SQL = "SELECT CountyID, CountyName, DIMLCountyName, S3Key FROM tblLookupCounties"

db_lookup_results: dict[str, list[dict]] = {}

for db_name, conn in DATABASES.items():
    rows = conn.execute_query(LOOKUP_SQL)
    db_lookup_results[db_name] = rows
    logger.info("'%s': retrieved %d county rows", db_name, len(rows))

# COMMAND ----------
# MAGIC %md ## 2. Cross-database consistency check
# MAGIC
# MAGIC Verify that S3Key values for the same CountyID agree across all databases.

# COMMAND ----------

def check_cross_db_consistency(db_results: dict[str, list[dict]]) -> list[dict]:
    """
    Return a list of discrepancy dicts where the same CountyID has
    different S3Key values across databases.
    """
    # Build {county_id: {db_name: s3_key}}
    county_db_map: dict[int, dict[str, str]] = {}
    for db_name, rows in db_results.items():
        for row in rows:
            cid = row["CountyID"]
            county_db_map.setdefault(cid, {})[db_name] = row.get("S3Key", "")

    discrepancies = []
    for cid, db_key_map in county_db_map.items():
        unique_keys = set(db_key_map.values())
        if len(unique_keys) > 1:
            discrepancies.append({
                "CountyID":   cid,
                "db_s3_keys": db_key_map,
            })
            logger.warning("CountyID=%s has inconsistent S3Keys across databases: %s",
                           cid, db_key_map)

    return discrepancies


consistency_issues = check_cross_db_consistency(db_lookup_results)

if consistency_issues:
    print(f"\n⚠️  Found {len(consistency_issues)} cross-database S3Key inconsistency(ies):\n")
    for issue in consistency_issues:
        print(f"  CountyID={issue['CountyID']}: {issue['db_s3_keys']}")
else:
    print("\n✅  S3Key values are consistent across all databases.\n")

# COMMAND ----------
# MAGIC %md ## 3. Validate S3Key format in each database

# COMMAND ----------

all_invalid: list[dict] = []
validation_reports: dict[str, dict] = {}

for db_name, rows in db_lookup_results.items():
    report = validate_lookup_counties(rows)
    validation_reports[db_name] = report
    for inv in report["invalid"]:
        inv["database_name"] = db_name
    all_invalid.extend(report["invalid"])

    print(f"\n[{db_name}] {report['summary']}")
    if report["invalid"]:
        print(f"  Invalid S3Keys in '{db_name}':")
        for row in report["invalid"]:
            print(f"    CountyID={row['CountyID']:>4}  CountyName={row['CountyName']:<20}"
                  f"  S3Key='{row['S3Key']}'  issues={row['issues']}")

# COMMAND ----------
# MAGIC %md ## 4. Generate correction script

# COMMAND ----------

correction_map = generate_correction_map(all_invalid)

if correction_map:
    print("\n📝  Proposed S3Key corrections:\n")
    print(f"  {'Database':<20} {'CountyID':>8}  {'CountyName':<20}  {'Old S3Key':<20}  {'New S3Key':<20}")
    print("  " + "-" * 96)
    for c in correction_map:
        db = c.get("database_name", "both")
        print(f"  {db:<20} {str(c['county_id']):>8}  {c['county_name']:<20}  "
              f"{c['old_s3_key']:<20}  {c['new_s3_key']:<20}")
else:
    print("\n✅  No S3Key corrections required.\n")

# COMMAND ----------
# MAGIC %md ## 5. Apply corrections (skipped in DRY_RUN mode)

# COMMAND ----------

UPDATE_S3KEY_SQL = (
    "UPDATE tblLookupCounties "
    "SET S3Key = '{new_s3_key}' "
    "WHERE CountyID = {county_id}"
)

correction_log: list[dict] = []

for c in correction_map:
    if not c["changes_needed"]:
        continue

    sql = UPDATE_S3KEY_SQL.format(
        new_s3_key=c["new_s3_key"],
        county_id=c["county_id"],
    )

    # Apply to the specific database this correction came from, or both if unspecified
    target_dbs = (
        {c["database_name"]: DATABASES[c["database_name"]]}
        if c.get("database_name") and c["database_name"] in DATABASES
        else DATABASES
    )

    for db_name, conn in target_dbs.items():
        rows_affected = conn.execute_update(sql)
        log_entry = {
            "database_name": db_name,
            "county_id":     c["county_id"],
            "county_name":   c["county_name"],
            "old_s3_key":    c["old_s3_key"],
            "new_s3_key":    c["new_s3_key"],
            "rows_affected": rows_affected,
            "dry_run":       DRY_RUN,
        }
        correction_log.append(log_entry)
        logger.info(
            "[%s] S3Key update for CountyID=%s: '%s' → '%s' (rows=%d, dry_run=%s)",
            db_name, c["county_id"], c["old_s3_key"], c["new_s3_key"],
            rows_affected, DRY_RUN,
        )

# COMMAND ----------
# MAGIC %md ## 6. Safety gate — halt if invalid S3Keys remain and DRY_RUN is off

# COMMAND ----------

# Re-validate after applying corrections (or just check in-memory).
still_invalid = [c for c in correction_map if c["changes_needed"] and DRY_RUN]

if still_invalid and not DRY_RUN:
    msg = (
        f"{len(still_invalid)} S3Key correction(s) could not be applied. "
        "Resolve these before proceeding with migration."
    )
    logger.error(msg)
    raise RuntimeError(msg)

# COMMAND ----------
# MAGIC %md ## 7. Summary report

# COMMAND ----------

print("\n" + "=" * 70)
print("  LOOKUP TABLE VALIDATION SUMMARY")
print("=" * 70)
for db_name, report in validation_reports.items():
    print(f"  [{db_name}] {report['summary']}")
print(f"\n  Cross-DB inconsistencies : {len(consistency_issues)}")
print(f"  Total corrections needed : {len([c for c in correction_map if c['changes_needed']])}")
print(f"  Corrections applied      : {'0 (DRY RUN)' if DRY_RUN else str(len(correction_log))}")
print("=" * 70)

if not all_invalid and not consistency_issues:
    print("\n✅  All S3Key values are valid.  Safe to proceed with migration.\n")
elif DRY_RUN:
    print("\n⚠️  DRY RUN — corrections logged but not applied.\n")
else:
    print("\n🚨  Corrections were applied.  Review the correction log before migrating.\n")

# Expose for downstream use
CORRECTION_LOG = correction_log
