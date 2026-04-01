from __future__ import annotations

import os
import csv
import itertools
import shutil
import logging
from pathlib import Path
from utils.database_utils import DatabaseConnection

def update_database_from_csv(csv_file_path: str):
    """
    Read migration results from CSV and update database accordingly.
    Rows are processed in batches of BATCH_SIZE (read from the environment,
    defaulting to 100) to reduce per-row transaction overhead.
    After processing, the CSV is moved to a processed_archive subdirectory
    alongside the source file, with a UTC timestamp appended to the filename.

    Parameters
    ----------
    csv_file_path : path to the CSV file with migration results
    """
    logger = logging.getLogger("LND-7726.db_update")

    batch_size = int(os.environ.get("BATCH_SIZE", 100))

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
        batch_num = 0
        while True:
            batch = list(itertools.islice(reader, batch_size))
            if not batch:
                break
            batch_num += 1

            success_rows = []
            failure_rows = []
            batch_skipped = 0

            for row in batch:
                processed = int(row['Processed'])
                if processed == 1:
                    success_rows.append(row)
                elif processed == -1:
                    failure_rows.append(row)
                else:
                    logger.warning(
                        f"Unknown Processed value {processed} for {row['record_id']}"
                    )
                    skipped += 1
                    batch_skipped += 1

            # --- Success bucket: one transaction, two executemany calls ---
            batch_successful = 0
            batch_failed = 0

            if success_rows:
                s3_params = [
                    (row['new_s3_path'], row['record_id']) for row in success_rows
                ]
                processed_params = [(row['record_id'],) for row in success_rows]
                try:
                    csd_conn.begin_transaction()
                    csd_conn.execute_many(
                        "UPDATE CS_Digital.dbo.tblS3Image WITH (ROWLOCK) "
                        "SET s3FilePath = ? WHERE recordID = ?",
                        s3_params,
                    )
                    csd_conn.execute_many(
                        "UPDATE CS_Digital.dbo.tblS3Image_LND7726 WITH (ROWLOCK) "
                        "SET Processed = 1 WHERE recordID = ?",
                        processed_params,
                    )
                    csd_conn.commit()
                    batch_successful += len(success_rows)
                    successful_updates += len(success_rows)
                except Exception as e:
                    csd_conn.rollback()
                    batch_failed += len(success_rows)
                    failed_updates += len(success_rows)
                    record_ids = [row['record_id'] for row in success_rows]
                    logger.error(
                        f"Batch {batch_num}: transaction rolled back for "
                        f"{len(success_rows)} record(s) {record_ids}: {e}"
                    )

            # --- Failure bucket: one executemany, no transaction ---
            if failure_rows:
                failure_params = [(row['record_id'],) for row in failure_rows]
                csd_conn.execute_many(
                    "UPDATE CS_Digital.dbo.tblS3Image_LND7726 WITH (ROWLOCK) "
                    "SET Processed = -1 WHERE recordID = ?",
                    failure_params,
                )
                batch_failed += len(failure_rows)
                failed_updates += len(failure_rows)

            logger.info(
                f"Batch {batch_num}: size={len(batch)}, "
                f"successful={batch_successful}, "
                f"failed={batch_failed}, "
                f"skipped={batch_skipped}"
            )

    csd_conn.close()

    logger.info(f"Database update complete:")
    logger.info(f"  Successful: {successful_updates}")
    logger.info(f"  Failed: {failed_updates}")
    logger.info(f"  Skipped: {skipped}")

    # Archive the processed CSV
    _archive_csv(csv_file_path, logger)


def _archive_csv(csv_file_path: str, logger: logging.Logger) -> None:
    """
    Move a processed CSV into a processed_archive directory that lives
    alongside the source file.  The archived file has a UTC timestamp
    appended to its stem so repeated runs never overwrite each other.

    Parameters
    ----------
    csv_file_path : original path of the CSV that was just processed
    logger        : caller's logger instance
    """
    source = Path(csv_file_path)
    archive_dir = source.parent / "processed_archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    shutil.move(str(source), archive_dir)
    logger.info(f"Moved processed CSV to: {archive_dir}")


if __name__ == '__main__':

    # Load .env file if python-dotenv is available (local / non-Databricks runs).
    try:
        from dotenv import load_dotenv

        _env_file = Path(r'C:/Users/donald.massey/PycharmProjects/LND-7726/.env')
        if _env_file.exists():
            load_dotenv(_env_file)
            print(f"Loaded environment variables from {_env_file}")
            for key, value in os.environ.items():
                print(f'key: {key}; value: {value}')
        else:
            print(f"No .env file found at {_env_file} — using environment / Databricks secrets.")
    except ImportError:
        print("python-dotenv not installed; skipping .env load.")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    target = Path(__file__).parent / "migration_results"
    if target.is_dir():
        csv_files = sorted(target.glob("migration_results_batch_*.csv"))
        if not csv_files:
            print(f"No batch CSV files found in {target}")
        for csv_file in csv_files:
            print(f"Processing: {csv_file.name}")
            update_database_from_csv(str(csv_file))
