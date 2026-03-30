from __future__ import annotations  # must be the FIRST import in the file

import os
import time
import logging
from pathlib import Path

from utils.s3_utils import S3Client
from utils.database_utils import DatabaseConnection


def main():
    logger = logging.getLogger("LND-7726.main")

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
    county_scans_title = get_db_connection(
        db_name=os.environ.get("CST_DB", None),
        server=os.environ.get("CST_SERVER", None),
        username=os.environ.get("CST_USERNAME", None),
        password=os.environ.get("CST_PASSWORD", None),
        dry_run=os.environ.get("DRY_RUN", True),
    )
    cs_digital = get_db_connection(
        db_name=os.environ.get("CSD_DB", None),
        server=os.environ.get("CSD_SERVER", None),
        username=os.environ.get("CSD_USERNAME", None),
        password=os.environ.get("CSD_PASSWORD", None),
        dry_run=os.environ.get("DRY_RUN", True),
    )

    DATABASES = {"CST_DB": county_scans_title, "CSD_DB": cs_digital}
    logger.info("Database connections ready: %s", list(DATABASES.keys()))

    csd_conn = DATABASES["CSD_DB"]
    csd_conn.connect()
    csd_list = csd_conn.execute_query("""SELECT tr.recordID, CONCAT(storageFilePath, '\\', tr.recordID, '.pdf') as source_file_path,
                                             new_s3FilePath as new_s3filepath
                                             FROM CS_Digital.dbo.tblrecord tr
                                             LEFT JOIN CS_Digital.dbo.tblS3Image_LND7726 s3 ON s3.recordID = tr.recordID
                                             WHERE s3.Processed = -1 AND storageFilePath LIKE '%smb.dc2isilon.na.drillinginfo.com%'""", params=[])
    csd_conn.close()
    s3_client = S3Client(bucket=os.environ.get("S3_BUCKET", None))

    exist_counter = 0
    not_exist_counter = 0
    for _ in csd_list:
        record_id = _["recordID"]
        new_s3filepath = _["new_s3filepath"]
        source_file_path = _["source_file_path"]
        logger.info(f"record_id: {record_id}, new_s3filepath: {new_s3filepath}, source_file_path: {source_file_path}")
        if os.path.exists(source_file_path):
        #     upload_result = s3_client.upload_file(source_file_path, new_s3filepath.replace(f"s3://{S3_BUCKET}/", "").lower())
        #     logger.info(f"Successfully uploaded {source_file_path} to {new_s3filepath}")
        #
        #     if upload_result:
        #         csd_conn.connect()
        #         csd_conn.execute_update(f"""UPDATE CS_Digital.dbo.tblS3Image WITH (ROWLOCK)
        #                                     SET s3FilePath = '{new_s3filepath}' WHERE recordID = '{record_id}'""", params=[])
        #         csd_conn.execute_update(f"""UPDATE CS_Digital.dbo.tblS3Image_LND7726 WITH (ROWLOCK)
        #                                     SET Processed = 1 WHERE recordID = '{record_id}'""", params=[])
        #         csd_conn.close()
            exist_counter += 1
        else:
        #     logger.info(f"DOES NOT EXIST record_id: {record_id}, new_s3filepath: {new_s3filepath}, source_file_path: {source_file_path}")
            not_exist_counter += 1
        # time.sleep(5)

    print(f"total record count: {len(csd_list)}; "
          f"exist_counter = {exist_counter}; "
          f"not_exist_counter = {not_exist_counter}")

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