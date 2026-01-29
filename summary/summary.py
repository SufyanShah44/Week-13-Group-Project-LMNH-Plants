"""
Partition plant readings by day and upload to S3 as Parquet.
"""

from os import environ as ENV
import os
import json
from io import BytesIO
import pandas as pd
import pyodbc
from dotenv import load_dotenv
import boto3

load_dotenv()


def get_sql_connection():
    """Returns a MS SQL Server connection"""
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
    Returns results from queried recordings table
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

    return pd.DataFrame.from_records(rows, columns=columns)


def _ensure_types(df):
    """
    Casts data to expected types to ensure data is processed as correct types
    """
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
        """
        Returns a summary dictionary
        """
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
        "top_plants_by_readings": per_plant.head(10)[["plant_id", "readings"]].to_dict(
            orient="records"
        ),
    }

    tables = {
        "per_plant": per_plant,
        "daily": daily,
    }

    return summary, tables


def write_outputs(summary, tables, out_dir="/tmp/out_summary"):
    """
    Writes outputs to files locally for inspection.
    Can be disabled if needed.
    """
    os.makedirs(out_dir, exist_ok=True)

    with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    tables["per_plant"].to_csv(os.path.join(
        out_dir, "per_plant.csv"), index=False)
    tables["daily"].to_csv(os.path.join(out_dir, "daily.csv"), index=False)


def get_s3_client():
    """
    Returns a boto3 client session if details are valid
    """

    region = ENV.get("AWS_REGION")
    if region:
        return boto3.client("s3", region_name=region)
    return boto3.client("s3")


def upload_daily_partitions_to_s3(daily_df, s3_prefix=None, filename="reading.parquet"):
    """
    Upload one parquet file per day:
      yyyy/mm/dd/<filename>
    """
    bucket = ENV.get("S3_BUCKET")
    if not bucket:
        raise ValueError("S3_BUCKET is not set in the environment.")

    if s3_prefix is None:
        s3_prefix = ENV.get("S3_PREFIX", "")

    prefix = (s3_prefix or "").strip("/")
    s3 = get_s3_client()

    df = daily_df.copy()
    if "day" not in df.columns:
        raise ValueError("daily_df must contain a 'day' column.")

    df["day"] = pd.to_datetime(df["day"], errors="coerce").dt.date
    if df["day"].isna().any():
        raise ValueError(
            "daily_df contains invalid 'day' values that could not be parsed.")

    uploaded = 0

    for _, row in df.iterrows():
        day = row["day"]
        yyyy = f"year={day.year:04d}"
        mm = f"month={day.month:02d}"
        dd = f"day={day.day:02d}"

        one = pd.DataFrame([row.to_dict()])

        buf = BytesIO()
        try:
            one.to_parquet(buf, index=False)
        except Exception as e:
            raise RuntimeError(
                "Failed to write Parquet. Ensure 'pyarrow' (recommended) or 'fastparquet' is installed."
            ) from e

        buf.seek(0)

        key_parts = [p for p in [prefix, yyyy, mm, dd, filename] if p]
        s3_key = "/".join(key_parts)

        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )
        uploaded += 1

    return uploaded


def truncate_table(connection, table="alpha.recordings"):
    """
    Truncate the source table.
    """
    cur = connection.cursor()
    cur.execute(f"TRUNCATE TABLE {table};")
    connection.commit()
    cur.close()
    connection.close()


def run_summary(df=None, table_name="alpha.recordings", s3_prefix=None, write_local_outputs=False):
    """
    Runs the summary process
    """
    if df is None:
        df = fetch_readings_from_db(table_name=table_name)

    summary, tables = summarise_readings(df)

    if write_local_outputs:
        write_outputs(summary, tables)

    uploaded = upload_daily_partitions_to_s3(
        tables["daily"],
        s3_prefix=s3_prefix,
        filename="reading.parquet",
    )

    return summary, tables, uploaded


def lambda_handler(event, context):
    """
    AWS Lambda entrypoint.
    """
    event = event or {}

    table_name = event.get("table_name", "alpha.recordings")
    s3_prefix = event.get("s3_prefix", ENV.get("S3_PREFIX", ""))
    truncate_after_upload = bool(event.get("truncate_after_upload", True))
    write_local_outputs = bool(event.get("write_local_outputs", False))

    summary, tables, uploaded = run_summary(
        df=None,
        table_name=table_name,
        s3_prefix=s3_prefix,
        write_local_outputs=write_local_outputs,
    )

    if truncate_after_upload:
        conn = get_sql_connection()
        truncate_table(conn, table=table_name)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Daily summaries uploaded to S3.",
                "source_table": table_name,
                "s3_bucket": ENV.get("S3_BUCKET"),
                "s3_prefix": s3_prefix,
                "days_uploaded": uploaded,
                "timestamp_range": summary.get("timestamp_range"),
                "rows_summarised": summary.get("rows"),
            }
        ),
    }


if __name__ == "__main__":
    run_summary(write_local_outputs=True)
