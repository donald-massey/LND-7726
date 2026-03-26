def process_record(batch):
    """
    Process a batch of records: copy, delete, and update DB.
    Must be a top-level function for pickling by multiprocessing.
    Each process creates its own S3 client and DB connection.
    """
    import os
    import logging
    from utils.database_utils import DatabaseConnection
    from utils.s3_utils import (
        S3Client,
        copy_and_verify)
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError

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

    def credentials_are_valid():
        """Check if current AWS credentials are still valid by making a lightweight API call."""
        try:
            sts = boto3.client('sts')
            identity = sts.get_caller_identity()
            # print(f"✅ Credentials valid — Account: {identity['Account']}, ARN: {identity['Arn']}")
            return True
        except (ClientError, NoCredentialsError) as e:
            print(f"❌ Credentials invalid: {e}")
            return False

    logger = logging.getLogger("LND-7726.process_record")
    from utils.database_utils import DatabaseConnection

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

    from utils.s3_utils import S3Client

    def get_s3_client() -> S3Client:
        """
        Return a boto3-backed S3 client for the configured bucket.

        AWS credentials are resolved via the standard boto3 credential chain:
        environment variables (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY),
        ~/.aws/credentials, or an IAM instance/service role.
        """
        region = os.environ.get("AWS_REGION", "us-east-1")
        return S3Client(bucket=os.environ.get("S3_BUCKET", None), region=region)


    s3_bucket = os.environ.get("S3_BUCKET")
    s3_client = S3Client(bucket=s3_bucket)
    logger.info("S3 client ready: bucket=%s", os.environ.get("S3_BUCKET", None))
    csd_conn = DATABASES["CSD_DB"]

    batch_results = []
    for row_dict in batch:
        record_id = row_dict["recordID"]
        old_s3_path = row_dict["old_s3FilePath"]
        new_s3_path = row_dict["new_s3FilePath"]

        if credentials_are_valid():
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
                , params=[])
                csd_conn.execute_update(
                    f"UPDATE CS_Digital.dbo.tblS3Image_LND7726 WITH (ROWLOCK) SET Processed = 1 WHERE recordID = '{record_id}'"
                , params=[])
                csd_conn.close()
                logger.info(f"record_id: {record_id} status: success")
                batch_results.append({"record_id": record_id, "status": "success"})

            except Exception as e:
                logger.error(f"Error processing {record_id}: {e}")
                try:
                    csd_conn.connect()
                    csd_conn.execute_update(
                        f"UPDATE CS_Digital.dbo.tblS3Image_LND7726 WITH (ROWLOCK) SET Processed = -1 WHERE recordID = '{record_id}'"
                    , params=[])
                    csd_conn.close()
                except Exception as db_err:
                    logger.error(f"Failed to update error status for {record_id}: {db_err}")
                batch_results.append({"record_id": record_id, "status": "error", "error": str(e)})
        else:
            batch_results.append({"record_id": record_id, "status": "Credentials Expired"})

    return batch_results