from __future__ import annotations  # must be the FIRST import in the file

import os
import logging
from pathlib import Path
from pebble import ProcessPool
from concurrent.futures import TimeoutError


def main():
    from utils.process_utils import process_record
    from utils.database_utils import DatabaseConnection

    logger = logging.getLogger("LND-7726.main")
    MAX_WORKERS = 8  # Adjust based on your system/resources
    TASK_TIMEOUT = 30  # seconds per task

    # ---------------------------------------------------------------------------
    # Config defaults
    # ---------------------------------------------------------------------------
    try:
        _ = DATABASES  # noqa: F821
        _ = S3Client  # noqa: F821
    except NameError:
        DRY_RUN = os.environ.get("DRY_RUN", "true").lower() in ("1", "true", "yes")
        DB_NAME_1 = os.environ.get("DB_NAME_1", "database_name_1")
        DB_NAME_2 = os.environ.get("DB_NAME_2", "database_name_2")
        DB_SERVER = os.environ.get("DB_SERVER", "")
        DB_USERNAME = os.environ.get("DB_USERNAME", "")
        DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
        S3_BUCKET = os.environ.get("S3_BUCKET", "enverus-courthouse-prod-chd-plants")
        DATABASES = {
            DB_NAME_1: DatabaseConnection(DB_NAME_1, DB_SERVER, DB_USERNAME, DB_PASSWORD, DRY_RUN),
            DB_NAME_2: DatabaseConnection(DB_NAME_2, DB_SERVER, DB_USERNAME, DB_PASSWORD, DRY_RUN),
        }

    def get_db_connection(*, db_name: str, server: str, username: str = "", password: str = "",
                          dry_run: bool = True, ) -> DatabaseConnection:
        """
        Return a live pyodbc database connection using explicit keyword arguments.
        """
        conn = DatabaseConnection(
            db_name=db_name,
            server=server,
            username=username,
            password=password,
            dry_run=dry_run,
        )
        conn.connect()
        return conn


    # Instantiate connections to both databases.
    countyScansTitle = get_db_connection(
        db_name=os.environ.get("CST_DB", None),
        server=os.environ.get("CST_SERVER", None),
        username=os.environ.get("CST_USERNAME", None),
        password=os.environ.get("CST_PASSWORD", None),
        dry_run=os.environ.get("DRY_RUN", True),
    )
    CS_Digital = get_db_connection(
        db_name=os.environ.get("CSD_DB", None),
        server=os.environ.get("CSD_SERVER", None),
        username=os.environ.get("CSD_USERNAME", None),
        password=os.environ.get("CSD_PASSWORD", None),
        dry_run=os.environ.get("DRY_RUN", True),
    )

    DATABASES = {"CST_DB": countyScansTitle, "CSD_DB": CS_Digital}
    logger.info("Database connections ready: %s", list(DATABASES.keys()))

    csd_conn = DATABASES["CSD_DB"]
    csd_conn.connect()
    csd_list = csd_conn.execute_query("SELECT TOP 10 * FROM tblS3Image_LND7726 WHERE Processed = 0", params=[])
    csd_conn.close()

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


if __name__ == '__main__':

    # Load .env file if python-dotenv is available (local / non-Databricks runs).
    try:
        from dotenv import load_dotenv

        _env_file = Path(r'C:\Users\donald.massey\PycharmProjects\LND-7726\.env')
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
    logger = logging.getLogger("LND-7726")
    logger.info("Logging initialised.")


    def log_section(title: str) -> None:
        """Print a clearly visible section header to the notebook output."""
        border = "=" * 70
        print(f"\n{border}")
        print(f"  {title}")
        print(f"{border}\n")

    main()