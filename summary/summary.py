"""
Partition plant readings by day and upload to S3 as Parquet.
"""

import os
from os import environ as ENV
from io import BytesIO

import boto3
import pandas as pd
import pyodbc
import pyarrow as pa
import pyarrow.parquet as pq
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
    """Returns results from queried recordings table"""
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


def ensure_types(df):
    """Casts to type to ensure data is processed as correct types"""
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


def get_s3_client():
    """Returns S3 client given valid credentials"""
    region = ENV.get("AWS_REGION")
    return boto3.client("s3", region_name=region)


def df_to_parquet_bytes(df):
    """
    Convert a DataFrame to Parquet bytes using pyarrow.
    """
    df = df[
        [
            "recording_id",
            "plant_id",
            "botanist_id",
            "timestamp",
            "soil_moisture",
            "temperature",
            "last_watered",
        ]
    ].copy()

    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    return buf.getvalue()


def build_s3_key(day, prefix):
    """
    day: datetime.date
    prefix: optional string
    """
    yyyy = f"{day.year:04d}"
    mm = f"{day.month:02d}"
    dd = f"{day.day:02d}"

    if prefix:
        prefix = prefix.strip("/")
        return f"{prefix}/year={yyyy}/month={mm}/day={dd}/reading.parquet"

    return f"year={yyyy}/month={mm}/day={dd}/reading.parquet"


def upload_partition(s3, bucket, key, parquet_bytes):
    """Uploads partition to S3 bucket"""
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=parquet_bytes,
        ContentType="application/octet-stream",
    )


def partition_and_upload(df, table_name="alpha.recordings"):
    """
    Partitions on timestamp date and uploads each partition as reading.parquet.
    Returns a small report dict for logging/monitoring.
    """
    df = ensure_types(df)

    # Only partition rows with a valid timestamp
    df = df.dropna(subset=["timestamp"]).copy()
    if df.empty:
        return {"partitions_uploaded": 0, "rows_uploaded": 0, "table": table_name}

    df["day"] = df["timestamp"].dt.date

    bucket = ENV["S3_BUCKET"]
    prefix = ENV.get("S3_PREFIX")
    s3 = get_s3_client()

    partitions_uploaded = 0
    rows_uploaded = 0

    # Deterministic order (useful for reproducible runs / debugging)
    for day, g in df.groupby("day", sort=True):
        parquet_bytes = df_to_parquet_bytes(g.drop(columns=["day"]))
        key = build_s3_key(day, prefix)
        upload_partition(s3, bucket, key, parquet_bytes)

        partitions_uploaded += 1
        rows_uploaded += int(len(g))

    return {
        "table": table_name,
        "partitions_uploaded": partitions_uploaded,
        "rows_uploaded": rows_uploaded,
        "s3_bucket": bucket,
        "s3_prefix": prefix,
    }


def main():
    """Main execution block"""
    table_name = ENV.get("RECORDINGS_TABLE", "alpha.recordings")
    df = fetch_readings_from_db(table_name=table_name)
    report = partition_and_upload(df, table_name=table_name)
    print(report)


def lambda_handler(event, context):
    """AWS Lambda entrypoint"""
    pass


if __name__ == "__main__":
    main()
