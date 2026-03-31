from __future__ import annotations  # must be the FIRST import in the file
"""
database_utils.py
=================
Database connection class and SQL helpers for the LND-7726
S3 County Folder Alignment migration.

Wraps a real pyodbc connection to a SQL Server instance.
Requires the ODBC Driver 18 for SQL Server to be installed.
"""

import ast
import time
import random
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
    """

    def __init__(
        self,
        db_name: str,
        server: str,
        username: str = "",
        password: str = ""
    ):
        self.db_name = db_name
        self.server = server
        self.username = username
        self.password = password
        self._conn = None
        self._in_transaction = False
        logger.info(
            "DatabaseConnection initialised: server=%s db=%s",
            server, db_name
        )

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open a pyodbc connection to the SQL Server database."""
        import pyodbc  # noqa: PLC0415

        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
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

    def execute_query(self, sql: str, params: list) -> list[dict[str, Any]]:
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

    def execute_update(self, sql: str, params: list, max_retries: int = 5) -> int:
        """
        Execute an INSERT / UPDATE / DELETE statement.

        Returns the number of rows affected (``cursor.rowcount``).

        Automatically retries on deadlock errors with exponential backoff + jitter.

        Parameters
        ----------
        sql         : SQL DML statement
        params      : optional positional parameters as a list (passed to pyodbc)
        max_retries : number of attempts before raising (default 3)
        """
        logger.info(
            "[%s] UPDATE : %s | params=%s",
            self.db_name, sql.strip(), params,
        )
        if self._conn is None:
            raise RuntimeError(
                f"[{self.db_name}] Not connected. Call connect() first."
            )

        for attempt in range(max_retries):
            try:
                cursor = self._conn.cursor()
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                cursor.commit()
                rows_affected = cursor.rowcount
                logger.info("[%s] %d row(s) affected.", self.db_name, rows_affected)
                return rows_affected

            except Exception as e:
                if "deadlock" in str(e).lower() and attempt < max_retries - 1:
                    wait = (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(
                        "[%s] Deadlock detected on attempt %d/%d, retrying in %.1fs: %s",
                        self.db_name, attempt + 1, max_retries, wait, sql.strip(),
                    )
                    time.sleep(wait)
                    # Reconnect in case the connection is in a bad state after deadlock
                    try:
                        self.close()
                        self.connect()
                    except Exception as reconnect_err:
                        logger.error(
                            "[%s] Failed to reconnect after deadlock: %s",
                            self.db_name, reconnect_err,
                        )
                        raise e from reconnect_err
                else:
                    logger.error(
                        "[%s] execute_update failed after %d attempt(s): %s",
                        self.db_name, attempt + 1, e,
                    )
                    raise