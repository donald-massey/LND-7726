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
        # for key, value in os.environ.items():
        #     print(f'key: {key}; value: {value}')
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


def get_db_connection(db_name: str) -> DatabaseConnection:
    """
    Return a live pyodbc database connection for *db_name*.
    """
    conn = DatabaseConnection(
        db_name=db_name,
        server=DB_SERVER,
        username=DB_USERNAME,
        password=DB_PASSWORD,
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
    return S3Client(bucket=S3_BUCKET, region=region)


s3_client = get_s3_client()
logger.info("S3 client ready: bucket=%s", S3_BUCKET)

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


# COMMAND ----------

# MAGIC %md ## 7. Configuration summary

# COMMAND ----------

log_section("Configuration Summary")
print(f"  S3 Bucket          : {S3_BUCKET}")
print(f"  State Prefix       : {STATE_PREFIX}")
print(f"  DB Server          : {DB_SERVER}")
print(f"  Database 1         : {DB_NAME_1}")
print(f"  Database 2         : {DB_NAME_2}")
print(f"  DRY RUN            : {DRY_RUN}")
print(f"  Batch Size         : {BATCH_SIZE}")
print(f"  Migration Map Path : {MIGRATION_MAP_PATH}")
print()
if DRY_RUN:
    print("  ⚠️  DRY RUN MODE — no changes will be written to S3 or databases.")
else:
    print("  🚨 LIVE MODE — changes WILL be written to S3 and databases.")

# COMMAND ----------

# MAGIC %md ## 8. Migration map parquet builder
# MAGIC
# MAGIC Use `create_migration_map_parquet()` to create (or overwrite) the parquet file
# MAGIC that drives county-folder corrections in Notebooks 5 and 6.
# MAGIC
# MAGIC **How to use in Databricks:**
# MAGIC 1. Edit the `CORRECTIONS` list in the cell below — one dict per county correction.
# MAGIC 2. Highlight the entire cell and click **Run → Run Selected Cell** (or Shift+Enter).
# MAGIC 3. The parquet file is written to `MIGRATION_MAP_PATH`.
# MAGIC 4. Re-run the cell any time you need to add, change, or remove rows.

# COMMAND ----------

# Required columns and their expected types — used for validation.
_MIGRATION_MAP_SCHEMA: dict[str, type] = {
    "database_name":     str,
    "county_id":         int,
    "county_name":       str,   # original (incorrect) county name, e.g. "CROCKETT2"
    "old_county_folder": str,   # S3 folder prefix to rename FROM, e.g. "crockett2"
    "new_county_folder": str,   # S3 folder prefix to rename TO,   e.g. "crockett"
}


def create_migration_map_parquet(
    entries: list[dict],
    path: str | None = None,
    overwrite: bool = True,
) -> str:
    """
    Write *entries* to a parquet file at *path* (defaults to ``MIGRATION_MAP_PATH``).

    Each entry must contain exactly the keys defined in ``_MIGRATION_MAP_SCHEMA``::

        {
            "database_name":     str,   # target database, e.g. "database_name_1"
            "county_id":         int,   # CountyID from tblLookupCounties
            "county_name":       str,   # original county name with suffix, e.g. "CROCKETT2"
            "old_county_folder": str,   # S3 folder segment to rename FROM, e.g. "crockett2"
            "new_county_folder": str,   # S3 folder segment to rename TO,   e.g. "crockett"
        }

    Parameters
    ----------
    entries:
        List of correction dicts.  Must not be empty.
    path:
        Destination file path.  Defaults to the ``MIGRATION_MAP_PATH`` widget value.
    overwrite:
        When ``True`` (default) the existing file is replaced.
        When ``False`` the call raises ``FileExistsError`` if the file already exists.

    Returns
    -------
    str
        The resolved path of the written parquet file.

    Raises
    ------
    ValueError
        If *entries* is empty or any entry is missing required keys.
    FileExistsError
        If *overwrite* is ``False`` and the file already exists.
    """
    import pandas as pd  # noqa: PLC0415

    dest = path or MIGRATION_MAP_PATH
    if not dest:
        raise ValueError(
            "No destination path provided and MIGRATION_MAP_PATH is not set. "
            "Pass a path argument or configure the 'migration_map_path' widget."
        )

    if not entries:
        raise ValueError("'entries' must not be empty.")

    required_keys = set(_MIGRATION_MAP_SCHEMA.keys())
    for i, entry in enumerate(entries):
        missing = required_keys - entry.keys()
        if missing:
            raise ValueError(
                f"Entry {i} is missing required key(s): {sorted(missing)}. "
                f"Expected keys: {sorted(required_keys)}"
            )

    if not overwrite and Path(dest).exists():
        raise FileExistsError(
            f"Migration map already exists at '{dest}'. "
            "Pass overwrite=True to replace it."
        )

    df = pd.DataFrame(entries, columns=list(_MIGRATION_MAP_SCHEMA.keys()))
    # Coerce types to match the schema
    df["county_id"] = df["county_id"].astype(int)
    for col in ("database_name", "county_name", "old_county_folder", "new_county_folder"):
        df[col] = df[col].astype(str)

    Path(dest).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dest, index=False)
    logger.info(
        "Migration map written to '%s' (%d row(s)).",
        dest, len(df),
    )
    print(f"✅  Migration map saved: {dest}  ({len(df)} row(s))")
    display_dataframe(df, title="Migration Map")
    return dest

# COMMAND ----------

# MAGIC %md ### Edit the CORRECTIONS list below, then run this cell to (re-)write the parquet file.

# COMMAND ----------

# ---------------------------------------------------------------------------
# ✏️  EDIT THIS LIST — one dict per county correction.
#     Run this cell (Shift+Enter) whenever you need to update the map.
# ---------------------------------------------------------------------------
CORRECTIONS: list[dict] = [
    # {
    #     "database_name":     "database_name_1",
    #     "county_id":         1,
    #     "county_name":       "CROCKETT2",
    #     "old_county_folder": "crockett2",
    #     "new_county_folder": "crockett",
    # },
    # {
    #     "database_name":     "database_name_2",
    #     "county_id":         2,
    #     "county_name":       "BEXAR1",
    #     "old_county_folder": "bexar1",
    #     "new_county_folder": "bexar",
    # },
    # Add more rows here …
]

if CORRECTIONS:
    create_migration_map_parquet(CORRECTIONS, path=MIGRATION_MAP_PATH, overwrite=True)
else:
    print(
        "ℹ️  CORRECTIONS list is empty — no parquet file written.\n"
        "    Add entries to CORRECTIONS above and re-run this cell."
    )
