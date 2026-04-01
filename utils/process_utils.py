import os
import csv
import logging
from pathlib import Path
from utils.s3_utils import (
    S3Client,
    copy_and_verify)
from botocore.exceptions import ClientError


def _write_batch_to_csv(batch_results: list[dict], csv_file: Path, max_retries: int = 8) -> None:
    """
    Append batch results to a shared CSV file with retry + exponential backoff.

    Mirrors the deadlock-retry pattern in DatabaseConnection.execute_update.
    Handles file contention from multiple processes writing to the same file.
    Uses fcntl file locking (Linux/Databricks) to prevent interleaved writes.
    """
    import csv
    import time
    import random
    import winfcntl
    import logging

    logger = logging.getLogger("LND-7726.process_record")
    fieldnames = ['record_id', 'old_s3_path', 'new_s3_path', 'Processed', 'error']

    for attempt in range(max_retries):
        try:
            with open(csv_file, 'a', newline='', encoding='utf-8') as f:
                # Acquire an exclusive lock — blocks until available
                winfcntl.flock(f, winfcntl.LOCK_EX)
                try:
                    # Only write header if file is empty (first writer)
                    f.seek(0, 2)  # seek to end
                    if f.tell() == 0:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                    else:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)

                    for result in batch_results:
                        writer.writerow({
                            'record_id': result.get('record_id', ''),
                            'old_s3_path': result.get('old_s3_path', ''),
                            'new_s3_path': result.get('new_s3_path', ''),
                            'Processed': result.get('Processed', -1),
                            'error': result.get('error', '')
                        })
                finally:
                    # Release the lock
                    winfcntl.flock(f, winfcntl.LOCK_UN)

            logger.info("Wrote %d results to %s", len(batch_results), csv_file)
            return  # success — exit retry loop

        except OSError as e:
            if attempt < max_retries - 1:
                wait = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(
                    "CSV write failed on attempt %d/%d, retrying in %.1fs: %s",
                    attempt + 1, max_retries, wait, e,
                )
                time.sleep(wait)
            else:
                logger.error(
                    "CSV write failed after %d attempt(s): %s",
                    max_retries, e,
                )
                raise

def process_record(batch):
    """
    Process a batch of records: copy, delete, and update DB.
    Must be a top-level function for pickling by multiprocessing.
    Each process creates its own S3 client and DB connection.
    """
    logger = logging.getLogger("LND-7726.process_record")

    s3_bucket = os.environ.get("S3_BUCKET")
    s3_client = S3Client(bucket=s3_bucket)
    logger.info("S3 client ready: bucket=%s", os.environ.get("S3_BUCKET", None))

    batch_results = []
    for row_dict in batch:
        record_id = row_dict["recordID"]
        old_s3_path = row_dict["old_s3FilePath"]
        new_s3_path = row_dict["new_s3FilePath"]

        try:
            # # Copy old_s3_path to new_s3_path
            # copy_result = copy_and_verify(client=s3_client, src_key=old_s3_path, dst_key=new_s3_path)
            # logger.info(f"copy_result: {copy_result}")
            #
            # # Delete old_s3_path
            # delete_result = s3_client.delete_object(
            #     Bucket=s3_bucket, Key=old_s3_path.replace(f"s3://{s3_bucket}/", "")
            # )
            # logger.info(f"delete_result: {delete_result}")

            logger.info(f"record_id: {record_id} status: success")
            batch_results.append({
                "record_id": record_id,
                "old_s3_path": old_s3_path,
                "new_s3_path": new_s3_path,
                "Processed": 1,  # Success
                "error": ""
            })
        except ClientError as e:
            # Check if this is a NoSuchKey error (missing source file in S3)
            error_code = e.response['Error']['Code']
            if error_code == 'ExpiredToken':
                print("Credentials have expired, need to refresh.")
                break
            elif error_code == 'NoSuchKey':
                logger.warning(f"Source file not found for {record_id}: {old_s3_path}")
                batch_results.append({
                    "record_id": record_id,
                    "old_s3_path": old_s3_path,
                    "new_s3_path": new_s3_path,
                    "Processed": -1,  # Failed - source not found
                    "error": str(e)
                })
            else:
                batch_results.append({
                    "record_id": record_id,
                    "old_s3_path": old_s3_path,
                    "new_s3_path": new_s3_path,
                    "Processed": -1,  # Failed
                    "error": str(e)
                })

    # Write ALL results to CSV (successes AND failures) with Processed flag
    output_dir = Path("migration_results")
    output_dir.mkdir(exist_ok=True)
    csv_file = output_dir / f"migration_results.csv"

    _write_batch_to_csv(batch_results, csv_file)

    return batch_results