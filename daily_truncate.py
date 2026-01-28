"""
Script to truncate alpha.recordings
"""

from __future__ import annotations

from os import environ as ENV
from typing import Dict

import pandas as pd
import pyodbc
from dotenv import load_dotenv


load_dotenv()


def get_sql_connection() -> pyodbc.Connection:
    """Create a SQL Server connection using environment variables."""
    conn_str = (f"DRIVER={{{ENV['DB_DRIVER']}}};SERVER={ENV['DB_HOST']};"
                f"PORT={ENV['DB_PORT']};DATABASE={ENV['DB_NAME']};"
                f"UID={ENV['DB_USERNAME']};PWD={ENV['DB_PASSWORD']};Encrypt=no;")

    return pyodbc.connect(conn_str)


def truncate_table(connection: pyodbc.Connection, table: str = "alpha.recordings") -> None:
    """Function to truncate the recordings table"""

    cur = connection.cursor()

    query = """
        TRUNCATE TABLE alpha.recordings
        ;
"""
    cur.execute(query)
    connection.commit()
    connection.close()


if __name__ == "__main__":
    load_dotenv()
    conn = get_sql_connection()
    truncate_table(conn)
