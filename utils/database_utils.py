"""
database_utils.py
=================
Database connection class and SQL helpers for the LND-7726
S3 County Folder Alignment migration.

In production these helpers wrap a real pyodbc / JDBC connection;
here they are wired to in-memory sample data so every notebook can
be exercised without a live SQL Server instance.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sample data — mirrors the schema used by the production databases.
# ---------------------------------------------------------------------------

SAMPLE_LOOKUP_COUNTIES = [
    {"CountyID": 1, "CountyName": "CROCKETT2", "DIMLCountyName": "CROCKETT_TX", "S3Key": "crockett"},
    {"CountyID": 2, "CountyName": "BEXAR1",    "DIMLCountyName": "BEXAR_TX",    "S3Key": "bexar"},
    {"CountyID": 3, "CountyName": "HARRIS3",   "DIMLCountyName": "HARRIS_TX",   "S3Key": "harris"},
    {"CountyID": 4, "CountyName": "TRAVIS",    "DIMLCountyName": "TRAVIS_TX",   "S3Key": "travis"},
]

SAMPLE_S3_IMAGE = [
    # CROCKETT2 — two records that need renaming
    {
        "recordID": 101,
        "s3FilePath": "s3://enverus-courthouse-prod-chd-plants/tx/crockett2/e1ed/e1edc1e1-8608-4f03-88a0-72844609af94.pdf",
    },
    {
        "recordID": 102,
        "s3FilePath": "s3://enverus-courthouse-prod-chd-plants/tx/crockett2/6af8/6af89658-4471-4668-81ea-e339b35eafa1.pdf",
    },
    # BEXAR1
    {
        "recordID": 201,
        "s3FilePath": "s3://enverus-courthouse-prod-chd-plants/tx/bexar1/aaaa/aaaaaaaa-1111-2222-3333-444444444444.pdf",
    },
    # HARRIS3
    {
        "recordID": 301,
        "s3FilePath": "s3://enverus-courthouse-prod-chd-plants/tx/harris3/bbbb/bbbbbbbb-5555-6666-7777-888888888888.pdf",
    },
    # TRAVIS — already correct, should not be migrated
    {
        "recordID": 401,
        "s3FilePath": "s3://enverus-courthouse-prod-chd-plants/tx/travis/cccc/cccccccc-9999-0000-aaaa-bbbbbbbbbbbb.pdf",
    },
]

SAMPLE_RECORD = [
    {"recordId": 101, "countyID": 1},
    {"recordId": 102, "countyID": 1},
    {"recordId": 201, "countyID": 2},
    {"recordId": 301, "countyID": 3},
    {"recordId": 401, "countyID": 4},
]


# ---------------------------------------------------------------------------
# Database Connection
# ---------------------------------------------------------------------------

class DatabaseConnection:
    """
    Database connection for a SQL Server instance via pyodbc or a Databricks JDBC source.

    All methods return in-memory sample data so notebooks can be
    developed and tested without a live database.
    """

    def __init__(self, db_name: str, server: str = "mock-server", dry_run: bool = True):
        self.db_name = db_name
        self.server = server
        self.dry_run = dry_run
        self._in_transaction = False
        logger.info("DatabaseConnection initialised: server=%s db=%s dry_run=%s",
                    server, db_name, dry_run)

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open (mock) connection — no-op in mock mode."""
        logger.info("[%s] Connected to %s", self.db_name, self.server)

    def close(self) -> None:
        """Close (mock) connection — no-op in mock mode."""
        logger.info("[%s] Connection closed", self.db_name)

    def begin_transaction(self) -> None:
        """Begin a transaction."""
        self._in_transaction = True
        logger.info("[%s] BEGIN TRANSACTION", self.db_name)

    def commit(self) -> None:
        """Commit the current transaction."""
        if not self._in_transaction:
            raise RuntimeError("No active transaction to commit.")
        self._in_transaction = False
        logger.info("[%s] COMMIT", self.db_name)

    def rollback(self) -> None:
        """Roll back the current transaction."""
        self._in_transaction = False
        logger.info("[%s] ROLLBACK", self.db_name)

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def execute_query(self, sql: str, params: dict | None = None) -> list[dict[str, Any]]:
        """
        Execute a SELECT query and return a list of row dicts.

        In mock mode the SQL is logged but sample data is returned
        based on simple keyword inspection.
        """
        logger.info("[%s] QUERY: %s | params=%s", self.db_name, sql.strip(), params)

        sql_upper = sql.upper()
        # Multi-table discovery join: tblS3Image + tblRecord + tblLookupCounties
        if "TBLS3IMAGE" in sql_upper and "TBLRECORD" in sql_upper and (
            "TBLLOOKUP" in sql_upper or "LOOKUPCOUNT" in sql_upper
        ):
            return _build_discovery_rows()
        # Single-table lookups (order matters — check more specific first)
        if "TBLLOOKUP" in sql_upper or "LOOKUPCOUNT" in sql_upper:
            return SAMPLE_LOOKUP_COUNTIES
        if "TBLS3IMAGE" in sql_upper and "TBLRECORD" in sql_upper:
            return _build_discovery_rows()
        if "TBLS3IMAGE" in sql_upper:
            return SAMPLE_S3_IMAGE
        if "TBLRECORD" in sql_upper:
            return SAMPLE_RECORD
        return []

    def execute_update(self, sql: str, params: dict | None = None) -> int:
        """
        Execute an INSERT / UPDATE / DELETE statement.

        In DRY_RUN mode the statement is logged but not applied.
        Returns the number of rows that would be / were affected.
        """
        logger.info("[%s] UPDATE (dry_run=%s): %s | params=%s",
                    self.db_name, self.dry_run, sql.strip(), params)
        if self.dry_run:
            logger.info("[%s] DRY RUN — no changes written.", self.db_name)
            return 0
        # Simulate a successful update affecting a small number of rows.
        simulated_rows = 2
        logger.info("[%s] %d row(s) affected.", self.db_name, simulated_rows)
        return simulated_rows


# ---------------------------------------------------------------------------
# Query builders
# ---------------------------------------------------------------------------

def build_discovery_query(state_prefix: str = "tx") -> str:
    """
    Return the SQL that identifies county folders with numeric suffixes
    whose path segment does not match the canonical S3Key.
    """
    return f"""
    SELECT
        i.s3FilePath,
        r.recordId,
        c.CountyID,
        c.CountyName,
        c.S3Key,
        c.DIMLCountyName
    FROM tblS3Image i
    JOIN tblRecord r ON r.recordId = i.recordID
    JOIN tblLookupCounties c ON r.countyID = c.CountyID
    WHERE i.s3FilePath LIKE '%/{state_prefix}/%'
      AND i.s3FilePath LIKE '%/' + c.CountyName + '/%'
      AND (
            c.CountyName LIKE '%1'
         OR c.CountyName LIKE '%2'
         OR c.CountyName LIKE '%3'
      )
      AND c.CountyName <> c.S3Key
    ORDER BY COUNT(*) OVER (PARTITION BY c.CountyID) DESC, c.CountyName
    """


def build_update_path_query(old_county_folder: str, new_county_folder: str, county_id: int) -> str:
    """
    Return the SQL that replaces old county folder segments in s3FilePath
    for a specific county (scoped by countyID to avoid cross-county edits).
    """
    return f"""
    UPDATE tblS3Image
    SET s3FilePath = REPLACE(s3FilePath, '/{old_county_folder}/', '/{new_county_folder}/')
    WHERE s3FilePath LIKE '%/{old_county_folder}/%'
      AND recordID IN (
          SELECT recordId FROM tblRecord WHERE countyID = {county_id}
      )
    """


def build_count_query(old_county_folder: str, county_id: int) -> str:
    """
    Return the SELECT COUNT(*) equivalent of build_update_path_query —
    used in DRY_RUN mode to preview the number of rows that would be updated.
    """
    return f"""
    SELECT COUNT(*) AS affected_rows
    FROM tblS3Image
    WHERE s3FilePath LIKE '%/{old_county_folder}/%'
      AND recordID IN (
          SELECT recordId FROM tblRecord WHERE countyID = {county_id}
      )
    """


# ---------------------------------------------------------------------------
# Batch update helper
# ---------------------------------------------------------------------------

def batch_update_paths(
    conn: DatabaseConnection,
    migration_map: list[dict],
    batch_size: int = 100,
) -> dict[str, int]:
    """
    Apply path updates for every county in *migration_map* using the
    supplied connection, committing after each batch.

    Parameters
    ----------
    conn           : database connection (real or mock)
    migration_map  : list of dicts with keys:
                       old_county_folder, new_county_folder, county_id
    batch_size     : rows per commit (not enforced in mock mode)

    Returns
    -------
    dict mapping county_id → rows_affected
    """
    results: dict[str, int] = {}

    for entry in migration_map:
        old_folder = entry["old_county_folder"]
        new_folder = entry["new_county_folder"]
        county_id  = entry["county_id"]

        if conn.dry_run:
            sql = build_count_query(old_folder, county_id)
        else:
            sql = build_update_path_query(old_folder, new_folder, county_id)

        try:
            conn.begin_transaction()
            rows = conn.execute_update(sql)
            conn.commit()
            results[str(county_id)] = rows
            logger.info(
                "County %s: %s → %s, rows_affected=%d",
                county_id, old_folder, new_folder, rows,
            )
        except Exception as exc:
            conn.rollback()
            logger.error("County %s update failed: %s", county_id, exc)
            results[str(county_id)] = -1

    return results


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_discovery_rows() -> list[dict[str, Any]]:
    """Join SAMPLE tables to produce discovery query result rows."""
    rows = []
    record_map = {r["recordId"]: r for r in SAMPLE_RECORD}
    county_map = {c["CountyID"]: c for c in SAMPLE_LOOKUP_COUNTIES}

    for img in SAMPLE_S3_IMAGE:
        rec = record_map.get(img["recordID"])
        if rec is None:
            continue
        county = county_map.get(rec["countyID"])
        if county is None:
            continue
        # Only return rows where CountyName != S3Key (the mismatches)
        if county["CountyName"] == county["S3Key"]:
            continue
        rows.append({
            "s3FilePath":     img["s3FilePath"],
            "recordId":       rec["recordId"],
            "CountyID":       county["CountyID"],
            "CountyName":     county["CountyName"],
            "S3Key":          county["S3Key"],
            "DIMLCountyName": county["DIMLCountyName"],
        })
    return rows
