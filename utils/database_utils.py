"""
database_utils.py
=================
Database connection class and SQL helpers for the LND-7726
S3 County Folder Alignment migration.

Wraps a real pyodbc connection to a SQL Server instance.
Requires the ODBC Driver 18 for SQL Server to be installed.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database Connection
# ---------------------------------------------------------------------------

class DatabaseConnection:
    """
    Database connection for a SQL Server instance via pyodbc.

    Parameters
    ----------
    db_name  : target database name
    server   : SQL Server hostname or IP
    username : SQL Server login
    password : SQL Server password
    dry_run  : when True, mutating statements are logged but not executed
    """

    def __init__(
        self,
        db_name: str,
        server: str,
        username: str = "",
        password: str = "",
        dry_run: bool = True,
    ):
        self.db_name = db_name
        self.server = server
        self.username = username
        self.password = password
        self.dry_run = dry_run
        self._conn = None
        self._in_transaction = False
        logger.info(
            "DatabaseConnection initialised: server=%s db=%s dry_run=%s",
            server, db_name, dry_run,
        )

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open a pyodbc connection to the SQL Server database."""
        import pyodbc  # noqa: PLC0415

        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.db_name};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate=yes;"
        )
        self._conn = pyodbc.connect(conn_str, autocommit=False)
        logger.info("[%s] Connected to %s", self.db_name, self.server)

    def close(self) -> None:
        """Close the pyodbc connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
        logger.info("[%s] Connection closed", self.db_name)

    def begin_transaction(self) -> None:
        """Mark the start of a logical transaction (pyodbc uses implicit transactions)."""
        self._in_transaction = True
        logger.info("[%s] BEGIN TRANSACTION", self.db_name)

    def commit(self) -> None:
        """Commit the current transaction."""
        if not self._in_transaction:
            raise RuntimeError("No active transaction to commit.")
        if self._conn:
            self._conn.commit()
        self._in_transaction = False
        logger.info("[%s] COMMIT", self.db_name)

    def rollback(self) -> None:
        """Roll back the current transaction."""
        if self._conn:
            self._conn.rollback()
        self._in_transaction = False
        logger.info("[%s] ROLLBACK", self.db_name)

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def execute_query(self, sql: str, params: list | None = None) -> list[dict[str, Any]]:
        """
        Execute a SELECT query and return a list of row dicts.

        Parameters
        ----------
        sql    : SQL SELECT statement
        params : optional positional parameters as a list (passed to pyodbc)
        """
        if self._conn is None:
            raise RuntimeError(
                f"[{self.db_name}] Not connected. Call connect() first."
            )
        logger.info("[%s] QUERY: %s | params=%s", self.db_name, sql.strip(), params)
        cursor = self._conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        if cursor.description is None:
            return []
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def execute_update(self, sql: str, params: list | None = None) -> int:
        """
        Execute an INSERT / UPDATE / DELETE statement.

        In DRY_RUN mode the statement is logged but not applied and 0 is returned.
        Otherwise returns the number of rows affected (``cursor.rowcount``).

        Parameters
        ----------
        sql    : SQL DML statement
        params : optional positional parameters as a list (passed to pyodbc)
        """
        logger.info(
            "[%s] UPDATE (dry_run=%s): %s | params=%s",
            self.db_name, self.dry_run, sql.strip(), params,
        )
        if self.dry_run:
            logger.info("[%s] DRY RUN — no changes written.", self.db_name)
            return 0
        if self._conn is None:
            raise RuntimeError(
                f"[{self.db_name}] Not connected. Call connect() first."
            )
        cursor = self._conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        rows_affected = cursor.rowcount
        logger.info("[%s] %d row(s) affected.", self.db_name, rows_affected)
        return rows_affected


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
    conn           : database connection
    migration_map  : list of dicts with keys:
                       old_county_folder, new_county_folder, county_id
    batch_size     : rows per commit (reserved for future use)

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
