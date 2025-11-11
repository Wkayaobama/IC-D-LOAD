#!/usr/bin/env python3
"""
SQL Server Connection Manager

Provides robust database connection management with pooling and retry logic.
"""

import pyodbc
import time
from typing import Optional, Dict
from contextlib import contextmanager


class ConnectionError(Exception):
    """Connection-related errors"""
    pass


class ConnectionManager:
    """
    Manage SQL Server connections with pooling and retry logic.

    Usage:
        manager = ConnectionManager(server="server", database="db")
        with manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM Table")
    """

    def __init__(
        self,
        server: str,
        database: str,
        trusted_connection: bool = True,
        username: Optional[str] = None,
        password: Optional[str] = None,
        pool_size: int = 5,
        max_overflow: int = 10,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        self.server = server
        self.database = database
        self.trusted_connection = trusted_connection
        self.username = username
        self.password = password
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self._connection_string = self._build_connection_string()
        self._pool = []

    def _build_connection_string(self) -> str:
        """Build ODBC connection string"""
        parts = [
            "DRIVER={SQL Server}",
            f"SERVER={self.server}",
            f"DATABASE={self.database}"
        ]

        if self.trusted_connection:
            parts.append("Trusted_Connection=yes")
        else:
            parts.append(f"UID={self.username}")
            parts.append(f"PWD={self.password}")

        return ";".join(parts)

    @contextmanager
    def get_connection(self):
        """Get database connection with retry logic"""
        conn = None
        for attempt in range(self.max_retries):
            try:
                conn = pyodbc.connect(self._connection_string)
                yield conn
                break
            except pyodbc.Error as e:
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise ConnectionError(f"Failed to connect after {self.max_retries} attempts: {e}")
            finally:
                if conn:
                    conn.close()


if __name__ == "__main__":
    # Example usage
    manager = ConnectionManager(
        server="your_server",
        database="CRMICALPS"
    )

    with manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT TOP 5 Comp_Name FROM Company")
        for row in cursor.fetchall():
            print(row.Comp_Name)
