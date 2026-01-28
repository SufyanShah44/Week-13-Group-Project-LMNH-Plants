"""
Produces summary metrics for plant readings data.
"""

import os
from os import environ as ENV
import json

import pandas as pd
import pyodbc
from dotenv import load_dotenv


load_dotenv()


def get_sql_connection():
    conn_str = (f"DRIVER={{{ENV['DB_DRIVER']}}};SERVER={ENV['DB_HOST']};"
                f"PORT={ENV['DB_PORT']};DATABASE={ENV['DB_NAME']};"
                f"UID={ENV['DB_USERNAME']};PWD={ENV['DB_PASSWORD']};Encrypt=no;")
    return pyodbc.connect(conn_str)


def fetch_readings_from_db(table_name="alpha.recordings"):
    """
    Fetch readings from SQL Server
    """
    query = f"""
        SELECT
            recording_id,
            plant_id,
            botanist_id,
            timestamp,
            soil_moisture,
            temperature,
            last_watered
        FROM {table_name};
    """

    conn = get_sql_connection()
    cursor = conn.cursor()
    cursor.execute(query)

    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    df = pd.DataFrame.from_records(rows, columns=columns)
    return df


def _ensure_types(df):
    df = df.copy()

    # Keep as nullable numeric where possible
    for col in ["recording_id", "plant_id", "botanist_id"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["soil_moisture", "temperature"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["timestamp", "last_watered"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def summarise_readings(df):
    """
    Returns:
      - summary dict
      - a dict of supporting DataFrames
    """
    df = _ensure_types(df)

    required = [
        "recording_id",
        "plant_id",
        "botanist_id",
        "timestamp",
        "soil_moisture",
        "temperature",
        "last_watered",
    ]
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    n_rows = int(len(df))
    n_plants = int(df["plant_id"].nunique(dropna=True))
    n_botanists = int(df["botanist_id"].nunique(dropna=True))

    ts_min = df["timestamp"].min()
    ts_max = df["timestamp"].max()

    # Missingness
    missingness = {}
    for c in required:
        missingness[c] = float(df[c].isna().mean())

    # Duplicate primary key check
    dup_recording_id = int(df["recording_id"].duplicated().sum())

    # Per-reading derived metric: days since last watered at time of recording
    df["days_since_last_watered"] = (
        df["timestamp"] - df["last_watered"]).dt.total_seconds() / 86400.0

    # Overall numeric summaries
    def num_summary(series):
        return {
            "count": int(series.notna().sum()),
            "mean": float(series.mean()) if series.notna().any() else None,
            "std": float(series.std()) if series.notna().any() else None,
            "min": float(series.min()) if series.notna().any() else None,
            "p25": float(series.quantile(0.25)) if series.notna().any() else None,
            "median": float(series.median()) if series.notna().any() else None,
            "p75": float(series.quantile(0.75)) if series.notna().any() else None,
            "max": float(series.max()) if series.notna().any() else None,
        }

    soil_stats = num_summary(df["soil_moisture"])
    temp_stats = num_summary(df["temperature"])
    watered_gap_stats = num_summary(df["days_since_last_watered"])

    # Flagging erroneous values
    flagged = {
        "negative_days_since_last_watered": int((df["days_since_last_watered"] < 0).sum(skipna=True)),
        "missing_timestamp": int(df["timestamp"].isna().sum()),
        "missing_last_watered": int(df["last_watered"].isna().sum()),
    }

    # Aggregates per plant and per botanist
    per_plant = (
        df.groupby("plant_id", dropna=True)
        .agg(
            readings=("recording_id", "count"),
            soil_mean=("soil_moisture", "mean"),
            soil_min=("soil_moisture", "min"),
            soil_max=("soil_moisture", "max"),
            temp_mean=("temperature", "mean"),
            temp_min=("temperature", "min"),
            temp_max=("temperature", "max"),
            last_seen=("timestamp", "max"),
        )
        .reset_index()
        .sort_values(["readings", "plant_id"], ascending=[False, True])
    )

    per_botanist = (
        df.groupby("botanist_id", dropna=True)
        .agg(
            readings=("recording_id", "count"),
            plants=("plant_id", pd.Series.nunique),
            first_seen=("timestamp", "min"),
            last_seen=("timestamp", "max"),
        )
        .reset_index()
        .sort_values(["readings", "botanist_id"], ascending=[False, True])
    )

    # Daily roll-up
    daily = (
        df.assign(day=df["timestamp"].dt.date)
        .groupby("day", dropna=True)
        .agg(
            readings=("recording_id", "count"),
            plants=("plant_id", pd.Series.nunique),
            botanists=("botanist_id", pd.Series.nunique),
            soil_mean=("soil_moisture", "mean"),
            temp_mean=("temperature", "mean"),
        )
        .reset_index()
        .sort_values("day")
    )

    summary = {
        "rows": n_rows,
        "unique_plants": n_plants,
        "unique_botanists": n_botanists,
        "timestamp_range": {
            "min": None if pd.isna(ts_min) else ts_min.isoformat(),
            "max": None if pd.isna(ts_max) else ts_max.isoformat(),
        },
        "missingness_rate": missingness,
        "duplicate_recording_id_count": dup_recording_id,
        "soil_moisture_summary": soil_stats,
        "temperature_summary": temp_stats,
        "days_since_last_watered_summary": watered_gap_stats,
        "flags": flagged,
        "top_plants_by_readings": per_plant.head(10)[["plant_id", "readings"]].to_dict(orient="records"),
        "top_botanists_by_readings": per_botanist.head(10)[["botanist_id", "readings", "plants"]].to_dict(orient="records"),
    }

    tables = {
        "per_plant": per_plant,
        "per_botanist": per_botanist,
        "daily": daily,
    }

    return summary, tables


def write_outputs(summary, tables, out_dir="out_summary"):
    os.makedirs(out_dir, exist_ok=True)

    with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    # CSVs for inspection or dashboards
    tables["per_plant"].to_csv(os.path.join(
        out_dir, "per_plant.csv"), index=False)
    tables["per_botanist"].to_csv(os.path.join(
        out_dir, "per_botanist.csv"), index=False)
    tables["daily"].to_csv(os.path.join(out_dir, "daily.csv"), index=False)


def run_summary(df=None, table_name="alpha.recordings"):
    """
    Main entry-point for your pipeline.

    - If df is provided: summarise it.
    - Otherwise: fetch from SQL Server and summarise that.
    """
    if df is None:
        df = fetch_readings_from_db(table_name=table_name)

    summary, tables = summarise_readings(df)
    write_outputs(summary, tables)
    return summary, tables


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
    run_summary()
    truncate_table(conn)
