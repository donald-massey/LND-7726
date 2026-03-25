# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1 — Migrate S3 Objects
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

# MAGIC %run ./0_setup_and_config

# COMMAND ----------

# DBTITLE 1,Imports and config defaults
import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
REPO_ROOT = Path(f"/Workspace{_nb_path}").parent.parent

# Ensure the repo root is on the Python path so `utils` can be imported.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.database_utils import DatabaseConnection
from utils.s3_utils import (
    S3Client,
    copy_and_verify)

logger = logging.getLogger("LND-7726.migrate_and_update")

# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------
try:
    _ = DATABASES  # noqa: F821
    _ = S3Client  # noqa: F821
except NameError:
    DRY_RUN      = os.environ.get("DRY_RUN", "true").lower() in ("1", "true", "yes")
    DB_NAME_1    = os.environ.get("DB_NAME_1", "database_name_1")
    DB_NAME_2    = os.environ.get("DB_NAME_2", "database_name_2")
    DB_SERVER    = os.environ.get("DB_SERVER", "")
    DB_USERNAME  = os.environ.get("DB_USERNAME", "")
    DB_PASSWORD  = os.environ.get("DB_PASSWORD", "")
    S3_BUCKET    = os.environ.get("S3_BUCKET", "enverus-courthouse-prod-chd-plants")
    DATABASES    = {
        DB_NAME_1: DatabaseConnection(DB_NAME_1, DB_SERVER, DB_USERNAME, DB_PASSWORD, DRY_RUN),
        DB_NAME_2: DatabaseConnection(DB_NAME_2, DB_SERVER, DB_USERNAME, DB_PASSWORD, DRY_RUN),
    }

# COMMAND ----------

# MAGIC %md ## Create CSD Dictionary

# COMMAND ----------

csd_conn = DATABASES["CSD_DB"]
csd_conn.connect()
csd_list = csd_conn.execute_query("SELECT TOP 1000000 * FROM tblS3Image_LND7726 WHERE Processed = 0")
csd_conn.close()

# COMMAND ----------

# MAGIC %md ## Process CS_Digital Dictionary

# COMMAND ----------

import os
from pebble import ProcessPool
from concurrent.futures import TimeoutError

s3_bucket = os.environ.get("S3_BUCKET", None)
MAX_WORKERS = 8  # Adjust based on your system/resources
TASK_TIMEOUT = 30  # seconds per task


def process_record(row_dict):
    """
    Process a single record: copy, delete, and update DB.
    Must be a top-level function for pickling by multiprocessing.
    Each process creates its own S3 client and DB connection.
    """
    record_id = row_dict["recordID"]
    old_s3_path = row_dict["old_s3FilePath"]
    new_s3_path = row_dict["new_s3FilePath"]
    processed = row_dict["Processed"]

    s3_client = S3Client(bucket=s3_bucket)
                
    try:
        # Copy old_s3_path to new_s3_path
        copy_result = copy_and_verify(client=s3_client, src_key=old_s3_path, dst_key=new_s3_path)
        logger.info(f"copy_result: {copy_result}")
        if "error" in copy_result:
            raise Exception(f"Error copying {old_s3_path} to {new_s3_path}: {copy_result['error']}")

        # Delete old_s3_path
        delete_result = s3_client.delete_object(
            Bucket=s3_bucket, Key=old_s3_path.replace(f"s3://{s3_bucket}/", "")
        )
        logger.info(f"delete_result: {delete_result}")

        # Update DB with new path and mark as processed
        csd_conn.connect()
        csd_conn.execute_update(
            f"UPDATE CS_Digital.dbo.tblS3Image WITH (ROWLOCK) SET s3FilePath = '{new_s3_path}' WHERE recordID = '{record_id}'"
        )
        csd_conn.execute_update(
            f"UPDATE CS_Digital.dbo.tblS3Image_LND7726 WITH (ROWLOCK) SET Processed = 1 WHERE recordID = '{record_id}'"
        )
        csd_conn.close()
        logger.info(f"record_id: {record_id} status: success")
        return {"record_id": record_id, "status": "success"}

    except Exception as e:
        logger.error(f"Error processing {record_id}: {e}")
        try:
            csd_conn.connect()
            csd_conn.execute_update(
                f"UPDATE CS_Digital.dbo.tblS3Image_LND7726 WITH (ROWLOCK) SET Processed = -1 WHERE recordID = '{record_id}'"
            )
            csd_conn.close()
        except Exception as db_err:
            logger.error(f"Failed to update error status for {record_id}: {db_err}")
        return {"record_id": record_id, "status": "error", "error": str(e)}


def main():
    results = []

    with ProcessPool(max_workers=MAX_WORKERS) as pool:
        future = pool.map(process_record, csd_list, timeout=TASK_TIMEOUT)
        iterator = future.result()

        while True:
            try:
                result = next(iterator)
                logger.info(f"Completed: {result}")
                results.append(result)
            except StopIteration:
                break
            except TimeoutError as e:
                logger.error(f"Task timed out: {e}")
                results.append({"status": "timeout", "error": str(e)})
            except Exception as e:
                logger.error(f"Task failed: {e}")
                results.append({"status": "error", "error": str(e)})

    succeeded = sum(1 for r in results if r.get("status") == "success")
    failed = len(results) - succeeded
    logger.info(f"Processing complete: {succeeded} succeeded, {failed} failed out of {len(csd_list)} total")


if __name__ == "__main__":
    main()

# COMMAND ----------

# MAGIC %md ## 1. Create Dictionary From Processing Table s3Image_LND7726 From CountyScansTitle

# COMMAND ----------

cst_conn = DATABASES["CST_DB"]
cst_conn.connect()
cst_list = cst_conn.execute_query("SELECT * FROM tblS3Image_LND7726 WHERE Processed = 0")
cst_conn.close()

# COMMAND ----------

# MAGIC %md ## 2. Process CountyScansTitle Dictionary

# COMMAND ----------

s3_bucket = os.environ.get("S3_BUCKET", None)
for row_dict in cst_list:
    record_id = row_dict["recordID"]
    old_s3_path = row_dict["old_s3FilePath"]
    new_s3_path = row_dict["new_s3FilePath"]
    processed = row_dict["Processed"]

    # print(f"RecordID: {record_id}")
    # print(f"old_s3_path: {old_s3_path}")
    # print(f"new_s3_path: {new_s3_path}")
    # print(f"processed: {processed}")

    try:
        # Copy old_s3_path to new_s3_path
        copy_result = copy_and_verify(client=s3_client, src_key=old_s3_path, dst_key=new_s3_path)
        logger.info(f'copy_result: {copy_result}')
        if 'error' in copy_result:
            raise Exception(f"Error copying {old_s3_path} to {new_s3_path}: {copy_result['error']}")
        # Delete old_s3_path
        delete_result = s3_client.delete_object(Bucket=s3_bucket, Key=old_s3_path.replace(f"s3://{s3_bucket}/", ""))
        logger.info(f'delete_result: {delete_result}')
        # Update tblS3Image with new_s3Path
        cst_conn.connect()
        cst_conn.execute_update(f"UPDATE countyScansTitle.dbo.tblS3Image SET s3FilePath = '{new_s3_path}' WHERE recordID = '{record_id}'")
        # Update tblS3Image_LND7726 with processed = 1
        cst_conn.execute_update(f"UPDATE countyScansTitle.dbo.tblS3Image_LND7726 SET Processed = 1 WHERE recordID = '{record_id}'")
        cst_conn.close()
    except Exception as e:
        logger.error(f"Error: {e}")
        cst_conn.connect()
        cst_conn.execute_update(f"UPDATE countyScansTitle.dbo.tblS3Image_LND7726 SET Processed = -1 WHERE recordID = '{record_id}'")
        cst_conn.close()
