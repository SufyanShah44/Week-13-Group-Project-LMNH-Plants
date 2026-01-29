"""
Transform script for soil and temperature readings.
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


def fetch_botanist_lookup(conn: pyodbc.Connection) -> Dict[str, int]:
    """
    Retrieve botanist_name via botanist_id mapping
    """
    query = """
        SELECT botanist_id, name
        FROM alpha.botanists;
    """

    cursor = conn.cursor()
    cursor.execute(query)

    lookup = {row.name: row.botanist_id for row in cursor.fetchall()}
    cursor.close()
    return lookup


def transform_readings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform readings DataFrame into load-ready format.
    Drops rows with extreme soil_moisture or temperature values.
    """
    conn = get_sql_connection()
    botanist_lookup = fetch_botanist_lookup(conn)
    conn.close()

    df = df.copy()

    df["botanist_id"] = df["botanist_name"].map(botanist_lookup)

    if df["botanist_id"].isna().any():
        missing = df.loc[df["botanist_id"].isna(), "botanist_name"].unique()
        raise ValueError(f"Missing botanist IDs for: {missing}")

    df["plant_id"] = df["plant_id"].astype("int64")
    df["botanist_id"] = df["botanist_id"].astype("int64")

    df["timestamp"] = pd.to_datetime(df["recording_taken"], errors="coerce")
    df["last_watered"] = pd.to_datetime(df["last_watered"], errors="coerce")

    df["soil_moisture"] = pd.to_numeric(df["soil_moisture"], errors="coerce")
    df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce")

    df = df[
        df["soil_moisture"].between(0, 100)
        & df["temperature"].between(0, 40)
    ]

    return df[
        [
            "plant_id",
            "botanist_id",
            "timestamp",
            "soil_moisture",
            "temperature",
            "last_watered",
        ]
    ]
