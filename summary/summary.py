"""
Produces summary metrics for plant readings data, writes local outputs,
and uploads the daily summary to S3 as partitioned Parquet files.
"""

import os
from os import environ as ENV
import json
from io import BytesIO

import pandas as pd
import pyodbc
import boto3
from dotenv import load_dotenv


load_dotenv()


def get_sql_connection():
    conn_str = (
        f"DRIVER={{{ENV['DB_DRIVER']}}};"
        f"SERVER={ENV['DB_HOST']};"
        f"PORT={ENV['DB_PORT']};"
        f"DATABASE={ENV['DB_NAME']};"
        f"UID={ENV['DB_USERNAME']};"
        f"PWD={ENV['DB_PASSWORD']};"
        "Encrypt=no;"
    )
    return pyodbc.connect(conn_str)


def fetch_readings_from_db(table_name="alpha.recordings"):
    """
    Fetch readings from SQL Server (without pandas SQL helpers).
    """
    query = f"""
        SELECT
            recording_id,
            plant_id,
            botanist_id,
            [timestamp],
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

    return pd.DataFrame.from_records(rows, columns=columns)


def _ensure_types(df):
    df = df.copy()

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
      - dict of supporting DataFrames
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

    missingness = {}
    for c in required:
        missingness[c] = float(df[c].isna().mean())

    dup_recording_id = int(df["recording_id"].duplicated().sum())

    df["days_since_last_watered"] = (
        df["timestamp"] - df["last_watered"]
    ).dt.total_seconds() / 86400.0

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

    flagged = {
        "negative_days_since_last_watered": int(
            (df["days_since_last_watered"] < 0).sum(skipna=True)
        ),
        "missing_timestamp": int(df["timestamp"].isna().sum()),
        "missing_last_watered": int(df["last_watered"].isna().sum()),
    }

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

    daily = (
        df.assign(day=df["timestamp"].dt.date)
        .groupby("day", dropna=True)
        .agg(
            readings=("recording_id", "count"),
            plants=("plant_id", pd.Series.nunique),
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
        "top_plants_by_readings": per_plant.head(10)[["plant_id", "readings"]].to_dict(
            orient="records"
        ),
    }

    tables = {
        "per_plant": per_plant,
        "daily": daily,
    }

    return summary, tables


def write_outputs(summary, tables, out_dir="out_summary"):
    os.makedirs(out_dir, exist_ok=True)

    with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    tables["per_plant"].to_csv(os.path.join(
        out_dir, "per_plant.csv"), index=False)
    tables["daily"].to_csv(os.path.join(out_dir, "daily.csv"), index=False)


def get_s3_client():
    region = ENV.get("AWS_REGION")
    if region:
        return boto3.client("s3", region_name=region)
    return boto3.client("s3")


def upload_daily_partitions_to_s3(daily_df):
    """
    Upload one parquet file per day:
      yyyy/mm/dd/reading.parquet
    """
    bucket = ENV.get("S3_BUCKET")
    if not bucket:
        raise ValueError("S3_BUCKET is not set in the environment.")

    prefix = ENV.get("S3_PREFIX").strip("/")
    s3 = get_s3_client()

    df = daily_df.copy()
    if "day" not in df.columns:
        raise ValueError("daily_df must contain a 'day' column.")

    df["day"] = pd.to_datetime(df["day"], errors="coerce").dt.date
    if df["day"].isna().any():
        raise ValueError(
            "daily_df contains invalid 'day' values that could not be parsed.")

    for _, row in df.iterrows():
        day = row["day"]
        yyyy = f"{day.year:04d}"
        mm = f"{day.month:02d}"
        dd = f"{day.day:02d}"

        one = pd.DataFrame([row.to_dict()])

        buf = BytesIO()
        try:
            one.to_parquet(buf, index=False)
        except Exception as e:
            raise RuntimeError(
                "Failed to write Parquet."
            ) from e

        buf.seek(0)

        key_parts = [p for p in [prefix, yyyy, mm, dd, "summary.parquet"] if p]
        s3_key = "/".join(key_parts)

        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )


def run_summary(df=None, table_name="alpha.recordings"):
    """
    Main entry-point.

    - If df is provided: summarise it.
    - Otherwise: fetch from SQL Server and summarise that.
    - Writes local JSON/CSVs and uploads daily parquet partitions to S3.
    """
    if df is None:
        df = fetch_readings_from_db(table_name=table_name)

    summary, tables = summarise_readings(df)
    write_outputs(summary, tables)
    upload_daily_partitions_to_s3(tables["daily"])

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
    run_summary()
