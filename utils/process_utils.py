from __future__ import annotations  # must be the FIRST import in the file

import os
import csv
import logging
from pathlib import Path
from datetime import datetime
from utils.s3_utils import (
    S3Client,
    copy_and_verify)
from botocore.exceptions import ClientError


def _write_batch_to_csv(batch_results: list[dict], csv_file: Path) -> None:
    """
    Write batch results to a dedicated CSV file (one file per batch).
    No locking needed since each batch writes to its own file.
    """
    logger = logging.getLogger("LND-7726.process_record")
    fieldnames = ['record_id', 'old_s3_path', 'new_s3_path', 'Processed', 'error']

    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for result in batch_results:
            writer.writerow({
                'record_id': result.get('record_id', ''),
                'old_s3_path': result.get('old_s3_path', ''),
                'new_s3_path': result.get('new_s3_path', ''),
                'Processed': result.get('Processed', -1),
                'error': result.get('error', '')
            })

    logger.info("Wrote %d results to %s", len(batch_results), csv_file)


def process_record(batch_tuple):
    """
    Process a batch of records: copy, delete, and update DB.
    Must be a top-level function for pickling by multiprocessing.
    Each process creates its own S3 client and DB connection.

    Parameters
    ----------
    batch_tuple : (batch_number, list[dict]) — batch number and the row dicts
    """
    batch_number, batch = batch_tuple

    logger = logging.getLogger("LND-7726.process_record")
    logger.info("Starting batch %d with %d records", batch_number, len(batch))

    s3_bucket = os.environ.get("S3_BUCKET")
    s3_client = S3Client(bucket=s3_bucket)
    logger.info("S3 client ready: bucket=%s", os.environ.get("S3_BUCKET", None))

    batch_results = []
    for row_dict in batch:
        record_id = row_dict["recordID"]
        old_s3_path = row_dict["old_s3FilePath"]
        new_s3_path = row_dict["new_s3FilePath"]

        try:
            # Copy old_s3_path to new_s3_path
            copy_result = copy_and_verify(client=s3_client, src_key=old_s3_path, dst_key=new_s3_path)
            logger.info(f"copy_result: {copy_result}")

            # Delete old_s3_path
            delete_result = s3_client.delete_object(
                Bucket=s3_bucket, Key=old_s3_path.replace(f"s3://{s3_bucket}/", "")
            )
            logger.info(f"delete_result: {delete_result}")

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
                    "Processed": -2,  # Failed
                    "error": str(e)
                })

    # Write results to a batch-specific CSV file
    output_dir = Path("migration_results")
    output_dir.mkdir(exist_ok=True)
    csv_file = output_dir / f"migration_results_batch_{batch_number}_{datetime.now().strftime('%Y-%m-%d')}.csv"

    _write_batch_to_csv(batch_results, csv_file)

    return batch_results