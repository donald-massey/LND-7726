# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 4 — Migrate S3 Objects
# MAGIC
# MAGIC **Purpose:** For each county in the migration map (largest first), copy all
# MAGIC S3 objects from the old county folder to the new county folder, verify each
# MAGIC copy, then delete the source objects.
# MAGIC
# MAGIC **Strategy:** Copy → Verify (ETag / size) → Delete (source)
# MAGIC
# MAGIC **Safety:**
# MAGIC   - DRY_RUN mode (default `True`) logs intended operations without writing.
# MAGIC   - Every operation is logged to the `county_migration_audit` Delta table.
# MAGIC   - The notebook is idempotent: if the destination already exists and the
# MAGIC     source is gone, the row is logged as `already_moved` and skipped.
# MAGIC
# MAGIC **Pre-requisite:** Notebooks 0–3 have completed successfully.

# COMMAND ----------

# %run ./0_setup_and_config   # ← uncomment in Databricks
# %run ./1_discovery_and_analysis

import sys
import os
import logging
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1] if "__file__" in dir() else Path("/Workspace/Repos/LND-7726")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.database_utils import DatabaseConnection
from utils.s3_utils import (
    S3Client,
    list_county_objects,
    get_s3_key_from_path,
    replace_county_folder,
    copy_and_verify,
)

logger = logging.getLogger("LND-7726.migrate_s3")

# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------
try:
    _ = DATABASES  # noqa: F821
    _ = MIGRATION_MAP  # noqa: F821
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
    BATCH_SIZE   = int(os.environ.get("BATCH_SIZE", "100"))
    DATABASES    = {
        DB_NAME_1: DatabaseConnection(DB_NAME_1, DB_SERVER, DB_USERNAME, DB_PASSWORD, DRY_RUN),
        DB_NAME_2: DatabaseConnection(DB_NAME_2, DB_SERVER, DB_USERNAME, DB_PASSWORD, DRY_RUN),
    }
    for _conn in DATABASES.values():
        _conn.connect()
    s3_client = S3Client(bucket=S3_BUCKET, region=os.environ.get("AWS_REGION", "us-east-1"))
    # Load migration map from parquet file (written by Notebook 0)
    _map_path = os.environ.get("MIGRATION_MAP_PATH", "")
    if not _map_path:
        raise RuntimeError(
            "MIGRATION_MAP not set. Run Notebooks 0–1 first, or set MIGRATION_MAP_PATH "
            "to the parquet file written by Notebook 0 (create_migration_map_parquet)."
        )
    import pandas as _pd
    MIGRATION_MAP = _pd.read_parquet(_map_path).to_dict(orient="records")

# COMMAND ----------
# MAGIC %md ## 1. Derive priority-ordered list of unique county migrations

# COMMAND ----------

from collections import Counter

county_counts: Counter = Counter()
for entry in MIGRATION_MAP:
    key = (entry["old_county_folder"], entry["new_county_folder"])
    county_counts[key] += 1

# Sort descending by record count (highest volume first)
priority_counties = sorted(county_counts.items(), key=lambda x: x[1], reverse=True)

print(f"\n📋  Unique county migrations ({len(priority_counties)}) — priority order:\n")
for (old_f, new_f), cnt in priority_counties:
    print(f"    {old_f:<20} → {new_f:<20}  ({cnt} DB record(s))")

# COMMAND ----------
# MAGIC %md ## 2. Migrate objects county by county

# COMMAND ----------

audit_log: list[dict] = []


def _audit(
    old_key: str,
    new_key: str,
    status: str,
    file_size: int = 0,
    error_message: str = "",
) -> dict:
    entry = {
        "old_key":       old_key,
        "new_key":       new_key,
        "status":        status,
        "timestamp":     datetime.now(tz=timezone.utc).isoformat(),
        "file_size":     file_size,
        "error_message": error_message,
        "dry_run":       DRY_RUN,
    }
    audit_log.append(entry)
    logger.info("AUDIT: status=%-14s  %s → %s", status, old_key, new_key)
    return entry


def migrate_county(
    client: S3Client,
    state_prefix: str,
    old_folder: str,
    new_folder: str,
    batch_size: int = 100,
) -> dict:
    """
    Move all objects under ``<state_prefix>/<old_folder>/`` to
    ``<state_prefix>/<new_folder>/``.

    Returns a summary dict with counts.
    """
    objects = list_county_objects(client, state_prefix, old_folder)

    if not objects:
        logger.warning("No objects found under '%s/%s/' — nothing to move.", state_prefix, old_folder)
        return {"old_folder": old_folder, "new_folder": new_folder,
                "copied": 0, "verified": 0, "deleted": 0, "skipped": 0, "errors": 0}

    logger.info("Migrating %d object(s): %s → %s", len(objects), old_folder, new_folder)

    counters = {"copied": 0, "already_moved": 0, "deleted": 0, "errors": 0}

    for batch_start in range(0, len(objects), batch_size):
        batch = objects[batch_start: batch_start + batch_size]

        for obj in batch:
            src_key  = obj["Key"]
            dst_key  = replace_county_folder(src_key, old_folder, new_folder)
            size     = obj.get("Size", 0)

            # --- Idempotency check ---
            src_exists = True
            dst_exists = False
            try:
                client.head_object(Bucket=client.bucket, Key=src_key)
            except FileNotFoundError:
                src_exists = False
            try:
                client.head_object(Bucket=client.bucket, Key=dst_key)
                dst_exists = True
            except FileNotFoundError:
                pass

            if not src_exists and dst_exists:
                _audit(src_key, dst_key, "already_moved", size)
                counters["already_moved"] += 1
                continue

            if not src_exists and not dst_exists:
                _audit(src_key, dst_key, "source_missing", size,
                       "Source not found and destination does not exist")
                counters["errors"] += 1
                continue

            # --- Copy ---
            copy_result = copy_and_verify(client, src_key, dst_key)
            if copy_result["status"] == "error":
                _audit(src_key, dst_key, "copy_error", size, copy_result.get("error", ""))
                counters["errors"] += 1
                continue

            # --- Verify copy (size and ETag) ---
            try:
                src_meta = client.head_object(Bucket=client.bucket, Key=src_key)
                dst_meta = client.head_object(Bucket=client.bucket, Key=dst_key)
                size_ok = dst_meta["ContentLength"] == size
                # ETags can differ when using multipart upload; compare when both available
                etag_ok = (
                    dst_meta.get("ETag") == src_meta.get("ETag")
                    if src_meta.get("ETag") and dst_meta.get("ETag")
                    else True
                )
                if not size_ok:
                    _audit(src_key, dst_key, "verify_failed", size,
                           f"size mismatch: expected {size}, got {dst_meta['ContentLength']}")
                    counters["errors"] += 1
                    continue
                if not etag_ok:
                    _audit(src_key, dst_key, "verify_failed", size,
                           f"etag mismatch: src={src_meta.get('ETag')} dst={dst_meta.get('ETag')}")
                    counters["errors"] += 1
                    continue
            except FileNotFoundError as exc:
                _audit(src_key, dst_key, "verify_failed", size, str(exc))
                counters["errors"] += 1
                continue

            counters["copied"] += 1
            _audit(src_key, dst_key, "copied", size)

            # --- Delete source ---
            try:
                client.delete_object(Bucket=client.bucket, Key=src_key)
                counters["deleted"] += 1
                _audit(src_key, dst_key, "deleted_source", size)
            except Exception as exc:
                _audit(src_key, dst_key, "delete_error", size, str(exc))
                counters["errors"] += 1

    return {
        "old_folder": old_folder,
        "new_folder": new_folder,
        **counters,
    }


migration_results: list[dict] = []

for (old_folder, new_folder), _ in priority_counties:
    if DRY_RUN:
        # In DRY_RUN mode: count objects and estimate work without moving anything
        objects = list_county_objects(s3_client, STATE_PREFIX, old_folder)
        total_size = sum(obj.get("Size", 0) for obj in objects)
        logger.info(
            "DRY RUN — would migrate %d object(s) (%.1f MB): %s → %s",
            len(objects), total_size / 1_048_576, old_folder, new_folder,
        )
        for obj in objects:
            _audit(
                obj["Key"],
                replace_county_folder(obj["Key"], old_folder, new_folder),
                "dry_run",
                obj.get("Size", 0),
            )
        migration_results.append({
            "old_folder":    old_folder,
            "new_folder":    new_folder,
            "copied":        len(objects),
            "already_moved": 0,
            "deleted":       0,
            "errors":        0,
        })
    else:
        result = migrate_county(
            s3_client,
            STATE_PREFIX,
            old_folder,
            new_folder,
            batch_size=BATCH_SIZE,
        )
        migration_results.append(result)

# COMMAND ----------
# MAGIC %md ## 3. Check for remaining objects in old folders

# COMMAND ----------

print("\n🔍  Checking for remaining objects in old county folders...\n")
leftovers_found = False

for (old_folder, _), _ in priority_counties:
    remaining = list_county_objects(s3_client, STATE_PREFIX, old_folder)
    if remaining:
        leftovers_found = True
        print(f"  ⚠️  {len(remaining)} object(s) still in '{STATE_PREFIX}/{old_folder}/':")
        for obj in remaining[:5]:
            print(f"       {obj['Key']}")
        if len(remaining) > 5:
            print(f"       … and {len(remaining) - 5} more")
    else:
        if not DRY_RUN:
            print(f"  ✅  '{STATE_PREFIX}/{old_folder}/' is empty.")

# COMMAND ----------
# MAGIC %md ## 4. Persist audit log to Delta table

# COMMAND ----------

AUDIT_TABLE = "county_migration_audit"

try:
    if spark and audit_log:  # noqa: F821
        df_audit = spark.createDataFrame(audit_log)  # noqa: F821
        (
            df_audit.write
            .format("delta")
            .mode("append")          # append so reruns accumulate history
            .option("mergeSchema", "true")
            .saveAsTable(AUDIT_TABLE)
        )
        logger.info("Audit log written to Delta table: %s (%d rows)", AUDIT_TABLE, len(audit_log))
    else:
        logger.info("Audit log held in memory (%d entries) — Spark not available.", len(audit_log))
except Exception as exc:
    logger.warning("Could not write audit Delta table: %s", exc)

# COMMAND ----------
# MAGIC %md ## 5. Migration report

# COMMAND ----------

print("\n" + "=" * 80)
print("  S3 MIGRATION REPORT")
print("=" * 80)
print(f"  {'Old Folder':<20} {'New Folder':<20} {'Copied':>7} {'Deleted':>7} {'Skipped':>7} {'Errors':>7}")
print("  " + "-" * 78)

total_copied  = 0
total_deleted = 0
total_skipped = 0
total_errors  = 0

for res in migration_results:
    copied  = res.get("copied", 0)
    deleted = res.get("deleted", 0)
    skipped = res.get("already_moved", 0)
    errors  = res.get("errors", 0)
    total_copied  += copied
    total_deleted += deleted
    total_skipped += skipped
    total_errors  += errors
    print(f"  {res['old_folder']:<20} {res['new_folder']:<20} "
          f"{copied:>7} {deleted:>7} {skipped:>7} {errors:>7}")

print("  " + "-" * 78)
print(f"  {'TOTAL':<42} {total_copied:>7} {total_deleted:>7} {total_skipped:>7} {total_errors:>7}")
print("=" * 80)

if DRY_RUN:
    print("\n  ⚠️  DRY RUN — no objects were actually moved.\n")
elif total_errors > 0:
    print(f"\n  🚨  {total_errors} error(s) occurred.  Review the audit log before updating the database.\n")
else:
    print("\n  ✅  All objects migrated successfully.  Proceed to Notebook 5.\n")

# Expose for downstream use
AUDIT_LOG        = audit_log
MIGRATION_RESULTS = migration_results
