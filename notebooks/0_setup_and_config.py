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

# MAGIC %md
# MAGIC ## Install mosdbcsql17

# COMMAND ----------

# MAGIC %sh
# MAGIC # Import the Microsoft GPG key in dearmored format to the location the repo expects
# MAGIC curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | sudo gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
# MAGIC
# MAGIC # Add the Microsoft SQL Server repo for Ubuntu 24.04 (noble)
# MAGIC echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/ubuntu/24.04/prod noble main" | sudo tee /etc/apt/sources.list.d/mssql-release.list
# MAGIC
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18

# COMMAND ----------

# MAGIC %md ## 1. Install / import dependencies

# COMMAND ----------

import os
import re
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone

# Derive REPO_ROOT from the notebook's actual workspace path.
# notebookPath() returns a workspace-relative path (e.g. /Users/.../LND-7726/notebooks/0_setup_and_config)
# Filesystem operations require the /Workspace prefix.
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
REPO_ROOT = Path(f"/Workspace{_nb_path}").parent.parent

# Ensure the repo root is on the Python path so `utils` can be imported.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Load .env file if python-dotenv is available (local / non-Databricks runs).
try:
    from dotenv import load_dotenv
    _env_file = REPO_ROOT / ".env"
    if _env_file.exists():
        load_dotenv(_env_file)
        print(f"Loaded environment variables from {_env_file}")
        for key, value in os.environ.items():
            print(f'key: {key}; value: {value}')
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

# MAGIC %md ## 3. Spark configuration

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

# MAGIC %md ## 4. Database connection factory

# COMMAND ----------

from utils.database_utils import DatabaseConnection


def get_db_connection(*, db_name: str, server: str, username: str = "", password: str = "", dry_run: bool = True,) -> DatabaseConnection:
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

# COMMAND ----------

# MAGIC %md ## 5. S3 connection factory

# COMMAND ----------

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


s3_client = get_s3_client()
logger.info("S3 client ready: bucket=%s", os.environ.get("S3_BUCKET", None))

# COMMAND ----------

# MAGIC %md ## 6. Shared utility functions

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

