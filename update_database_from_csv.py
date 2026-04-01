from __future__ import annotations

import os
import csv
import shutil
import logging
from pathlib import Path
from datetime import datetime
from utils.database_utils import DatabaseConnection

def update_database_from_csv(csv_file_path: str):
    """
    Read migration results from CSV and update database accordingly.
    After processing, the CSV is copied to a processed_archive subdirectory
    alongside the source file, with a UTC timestamp appended to the filename.

    Parameters
    ----------
    csv_file_path : path to the CSV file with migration results
    """
    logger = logging.getLogger("LND-7726.db_update")

    # Connect to database
    csd_conn = DatabaseConnection(
        db_name=os.environ.get("CSD_DB", None),
        server=os.environ.get("CSD_SERVER", None),
        username=os.environ.get("CSD_USERNAME", None),
        password=os.environ.get("CSD_PASSWORD", None)
    )
    csd_conn.connect()

    successful_updates = 0
    failed_updates = 0
    skipped = 0

    with open(csv_file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            record_id = row['record_id']
            new_s3_path = row['new_s3_path']
            processed = int(row['Processed'])
            error = row.get('error', '')

            try:
                if processed == 1:
                    # Successful migration: update s3FilePath and mark Processed = 1
                    rows_affected = csd_conn.execute_update(
                        f"UPDATE CS_Digital.dbo.tblS3Image WITH (ROWLOCK) SET s3FilePath = ? WHERE recordID = ?",
                        params=[new_s3_path, record_id]
                    )
                    csd_conn.execute_update(
                        f"UPDATE CS_Digital.dbo.tblS3Image_LND7726 WITH (ROWLOCK) SET Processed = 1 WHERE recordID = ?",
                        params=[record_id]
                    )
                    successful_updates += 1
                    logger.info(f"Updated {record_id}: {rows_affected} rows affected")

                elif processed == -1:
                    # Failed migration: mark Processed = -1 in tracking table only
                    csd_conn.execute_update(
                        f"UPDATE CS_Digital.dbo.tblS3Image_LND7726 WITH (ROWLOCK) SET Processed = -1 WHERE recordID = ?",
                        params=[record_id]
                    )
                    failed_updates += 1
                    logger.info(f"Marked {record_id} as failed (Processed=-1): {error}")

                else:
                    # Unknown Processed value
                    logger.warning(f"Unknown Processed value {processed} for {record_id}")
                    skipped += 1

            except Exception as e:
                failed_updates += 1
                logger.error(f"Failed to update {record_id}: {e}")

    csd_conn.close()

    logger.info(f"Database update complete:")
    logger.info(f"  Successful: {successful_updates}")
    logger.info(f"  Failed: {failed_updates}")
    logger.info(f"  Skipped: {skipped}")

    # Archive the processed CSV
    _archive_csv(csv_file_path, logger)


def _archive_csv(csv_file_path: str, logger: logging.Logger) -> None:
    """
    Copy a processed CSV into a processed_archive directory that lives
    alongside the source file.  The archived copy has a UTC timestamp
    appended to its stem so repeated runs never overwrite each other.

    Parameters
    ----------
    csv_file_path : original path of the CSV that was just processed
    logger        : caller's logger instance
    """
    source = Path(csv_file_path)
    archive_dir = source.parent / "processed_archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    archived_name = f"{source.stem}_{timestamp}{source.suffix}"
    destination = archive_dir / archived_name

    shutil.copy2(source, destination)
    logger.info(f"Archived processed CSV to: {destination}")


if __name__ == '__main__':

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    target = Path(__file__, 'migration_results')
    if target.is_dir():
        csv_files = sorted(target.glob("migration_results_batch_*.csv"))
        if not csv_files:
            print(f"No batch CSV files found in {target}")
        for csv_file in csv_files:
            print(f"Processing {csv_file.name}...")
            update_database_from_csv(str(csv_file))
