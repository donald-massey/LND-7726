# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 0 — Setup and Configuration
# MAGIC
# MAGIC **Purpose:** Load environment variables, initialise Spark, wire up mock (or real)
# MAGIC database and S3 connections, and expose shared helper functions used by all
# MAGIC subsequent notebooks.
# MAGIC
# MAGIC **Run this notebook first by calling `%run ./0_setup_and_config` from every
# MAGIC downstream notebook.**

# COMMAND ----------
# MAGIC %md ## 1. Install / import dependencies

# COMMAND ----------

import os
import sys
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

# Ensure the repo root is on the Python path so `utils` can be imported.
REPO_ROOT = Path(__file__).resolve().parents[1] if "__file__" in dir() else Path("/Workspace/Repos/LND-7726")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Load .env file if python-dotenv is available (local / non-Databricks runs).
try:
    from dotenv import load_dotenv
    _env_file = REPO_ROOT / ".env"
    if _env_file.exists():
        load_dotenv(_env_file)
        print(f"Loaded environment variables from {_env_file}")
    else:
        print(f"No .env file found at {_env_file} — using environment / Databricks secrets.")
except ImportError:
    print("python-dotenv not installed; skipping .env load.")

# COMMAND ----------
# MAGIC %md ## 2. Logging configuration

# COMMAND ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("LND-7726")
logger.info("Logging initialised.")

# COMMAND ----------
# MAGIC %md ## 3. Databricks widgets (configuration parameters)
# MAGIC
# MAGIC Widgets are displayed in the Databricks UI as form controls.
# MAGIC When running locally the values fall back to environment variables or defaults.

# COMMAND ----------

def _get_widget_or_env(widget_name: str, env_var: str, default: str) -> str:
    """
    Retrieve a value from a Databricks widget, environment variable, or hard-coded default.
    Falls back gracefully when running outside Databricks.
    """
    try:
        # dbutils is injected by the Databricks runtime.
        dbutils.widgets.text(widget_name, os.environ.get(env_var, default))  # noqa: F821
        return dbutils.widgets.get(widget_name)  # noqa: F821
    except NameError:
        # Not running in Databricks — use env var or default.
        return os.environ.get(env_var, default)


S3_BUCKET    = _get_widget_or_env("s3_bucket",    "S3_BUCKET",    "enverus-courthouse-prod-chd-plants")
STATE_PREFIX = _get_widget_or_env("state_prefix", "STATE_PREFIX", "tx")
DB_SERVER    = _get_widget_or_env("db_server",    "DB_SERVER",    "your-dev-server.database.windows.net")
DB_NAME_1    = _get_widget_or_env("db_name_1",    "DB_NAME_1",    "database_name_1")
DB_NAME_2    = _get_widget_or_env("db_name_2",    "DB_NAME_2",    "database_name_2")
DB_USERNAME  = _get_widget_or_env("db_username",  "DB_USERNAME",  "your_username")
DB_PASSWORD  = _get_widget_or_env("db_password",  "DB_PASSWORD",  "your_password")
DRY_RUN_STR  = _get_widget_or_env("dry_run",      "DRY_RUN",      "true")
BATCH_SIZE   = int(_get_widget_or_env("batch_size", "BATCH_SIZE",  "100"))

# Resolve DRY_RUN to a boolean
DRY_RUN: bool = DRY_RUN_STR.strip().lower() in ("1", "true", "yes")

logger.info(
    "Config: bucket=%s state=%s db_server=%s db1=%s db2=%s dry_run=%s batch_size=%d",
    S3_BUCKET, STATE_PREFIX, DB_SERVER, DB_NAME_1, DB_NAME_2, DRY_RUN, BATCH_SIZE,
)

# COMMAND ----------
# MAGIC %md ## 4. Spark configuration

# COMMAND ----------

try:
    spark  # noqa: F821  — available in Databricks
    logger.info("Using existing Databricks SparkSession.")
except NameError:
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("LND-7726-county-alignment").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession created locally.")
    except ImportError:
        spark = None
        logger.warning("PySpark not available — dataframe operations will be skipped.")

# COMMAND ----------
# MAGIC %md ## 5. Database connection factory

# COMMAND ----------

from utils.database_utils import DatabaseConnection


def get_db_connection(db_name: str) -> DatabaseConnection:
    """
    Return a database connection for *db_name*.

    Swap the sample-data implementation for a real pyodbc / JDBC wrapper
    once live credentials are available.
    """
    conn = DatabaseConnection(
        db_name=db_name,
        server=DB_SERVER,
        dry_run=DRY_RUN,
    )
    conn.connect()
    return conn


# Instantiate connections to both databases.
db1 = get_db_connection(DB_NAME_1)
db2 = get_db_connection(DB_NAME_2)

DATABASES = {DB_NAME_1: db1, DB_NAME_2: db2}
logger.info("Database connections ready: %s", list(DATABASES.keys()))

# COMMAND ----------
# MAGIC %md ## 6. Mock S3 connection factory

# COMMAND ----------

from utils.s3_utils import MockS3Client


def get_s3_client() -> MockS3Client:
    """
    Return an S3 client.

    Replace *MockS3Client* with a real boto3 client when AWS credentials
    are available:

        import boto3
        return boto3.client(
            "s3",
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
        )
    """
    return MockS3Client(bucket=S3_BUCKET, dry_run=DRY_RUN)


s3_client = get_s3_client()
logger.info("S3 client ready: bucket=%s", S3_BUCKET)

# COMMAND ----------
# MAGIC %md ## 7. Shared utility functions

# COMMAND ----------

def log_section(title: str) -> None:
    """Print a clearly visible section header to the notebook output."""
    border = "=" * 70
    print(f"\n{border}")
    print(f"  {title}")
    print(f"{border}\n")


def display_dataframe(df, title: str = "", max_rows: int = 50) -> None:
    """
    Display a Spark or pandas DataFrame in a notebook-friendly way.
    Falls back to pretty-printed dicts when neither framework is available.
    """
    if title:
        log_section(title)
    try:
        # Spark DataFrame
        df.show(max_rows, truncate=False)
    except AttributeError:
        try:
            # Pandas DataFrame
            from IPython.display import display
            display(df.head(max_rows))
        except Exception:
            for row in (df if isinstance(df, list) else []):
                print(row)


def rows_to_spark_df(rows: list[dict], schema=None):
    """Convert a list of dicts to a Spark DataFrame (if Spark is available)."""
    if spark is None:
        logger.warning("Spark not available — returning raw list.")
        return rows
    return spark.createDataFrame(rows, schema=schema) if rows else spark.createDataFrame([], schema)


# COMMAND ----------
# MAGIC %md ## 8. Configuration summary

# COMMAND ----------

log_section("Configuration Summary")
print(f"  S3 Bucket    : {S3_BUCKET}")
print(f"  State Prefix : {STATE_PREFIX}")
print(f"  DB Server    : {DB_SERVER}")
print(f"  Database 1   : {DB_NAME_1}")
print(f"  Database 2   : {DB_NAME_2}")
print(f"  DRY RUN      : {DRY_RUN}")
print(f"  Batch Size   : {BATCH_SIZE}")
print()
if DRY_RUN:
    print("  ⚠️  DRY RUN MODE — no changes will be written to S3 or databases.")
else:
    print("  🚨 LIVE MODE — changes WILL be written to S3 and databases.")
