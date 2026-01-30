"""Test suite for summary generation script"""
# pylint: skip-file
import json
import pandas as pd
import pytest
import summary as m

# Helpers / fakes


class FakeS3:
    def __init__(self):
        self.put_calls = []

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


class DummyCursor:
    def __init__(self):
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)

    def close(self):
        pass


class DummyConn:
    def __init__(self):
        self.cursor_obj = DummyCursor()
        self.committed = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


def test_ensure_types_casts_expected_columns(readings_df):
    out = m._ensure_types(readings_df)

    assert str(out["recording_id"].dtype) == "Int64"
    assert str(out["plant_id"].dtype) == "Int64"
    assert str(out["botanist_id"].dtype) == "Int64"

    assert pd.api.types.is_float_dtype(out["soil_moisture"])
    assert pd.api.types.is_float_dtype(out["temperature"])

    assert pd.api.types.is_datetime64_any_dtype(out["timestamp"])
    assert pd.api.types.is_datetime64_any_dtype(out["last_watered"])

    assert pd.isna(out.loc[2, "timestamp"])
    assert pd.isna(out.loc[2, "soil_moisture"])
    assert pd.isna(out.loc[2, "last_watered"])


def test_summarise_readings_requires_columns(readings_df):
    df = readings_df.drop(columns=["temperature"])

    with pytest.raises(ValueError) as excinfo:
        m.summarise_readings(df)

    assert "Missing required columns" in str(excinfo.value)
    assert "temperature" in str(excinfo.value)


def test_summarise_readings_returns_expected_keys(readings_df):
    summary, tables = m.summarise_readings(readings_df)

    assert summary["rows"] == 3
    assert summary["unique_plants"] == 2
    assert summary["unique_botanists"] == 2

    assert "timestamp_range" in summary
    assert "missingness_rate" in summary
    assert "duplicate_recording_id_count" in summary
    assert "soil_moisture_summary" in summary
    assert "temperature_summary" in summary
    assert "days_since_last_watered_summary" in summary
    assert "flags" in summary
    assert "top_plants_by_readings" in summary

    assert set(tables.keys()) == {"per_plant", "daily"}
    assert isinstance(tables["per_plant"], pd.DataFrame)
    assert isinstance(tables["daily"], pd.DataFrame)

    assert "day" in tables["daily"].columns


def test_summarise_readings_flags_negative_days_since_last_watered(readings_df):
    summary, _ = m.summarise_readings(readings_df)

    assert summary["flags"]["negative_days_since_last_watered"] >= 1


def test_summarise_readings_duplicate_recording_id_count():
    df = pd.DataFrame(
        [
            {
                "recording_id": 1,
                "plant_id": 10,
                "botanist_id": 100,
                "timestamp": "2025-01-01 10:00:00",
                "soil_moisture": 50,
                "temperature": 20,
                "last_watered": "2024-12-31 10:00:00",
            },
            {
                "recording_id": 1,
                "plant_id": 11,
                "botanist_id": 101,
                "timestamp": "2025-01-02 10:00:00",
                "soil_moisture": 60,
                "temperature": 21,
                "last_watered": "2025-01-01 10:00:00",
            },
        ]
    )

    summary, _ = m.summarise_readings(df)
    assert summary["duplicate_recording_id_count"] == 1


def test_upload_daily_partitions_requires_bucket(monkeypatch):
    monkeypatch.delenv("S3_BUCKET", raising=False)

    df = pd.DataFrame([{"day": "2025-01-01", "readings": 1}])

    with pytest.raises(ValueError) as excinfo:
        m.upload_daily_partitions_to_s3(df)

    assert "S3_BUCKET is not set" in str(excinfo.value)


def test_upload_daily_partitions_requires_day_column(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "my-bucket")

    with pytest.raises(ValueError) as excinfo:
        m.upload_daily_partitions_to_s3(pd.DataFrame([{"x": 1}]))

    assert "must contain a 'day' column" in str(excinfo.value)


def test_upload_daily_partitions_rejects_unparsable_day(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "my-bucket")

    df = pd.DataFrame([{"day": "not-a-date", "readings": 1}])

    with pytest.raises(ValueError) as excinfo:
        m.upload_daily_partitions_to_s3(df)

    assert "invalid 'day' values" in str(excinfo.value)


def test_upload_daily_partitions_uploads_one_object_per_day(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "my-bucket")
    monkeypatch.setenv("S3_PREFIX", "alpha/prefix")

    fake_s3 = FakeS3()
    monkeypatch.setattr(m, "get_s3_client", lambda: fake_s3)

    daily_df = pd.DataFrame(
        [
            {"day": "2025-01-01", "readings": 2, "plants": 1,
                "botanists": 1, "soil_mean": 52.0, "temp_mean": 20.5},
            {"day": "2025-01-02", "readings": 1, "plants": 1,
                "botanists": 1, "soil_mean": 49.0, "temp_mean": 19.0},
        ]
    )

    uploaded = m.upload_daily_partitions_to_s3(
        daily_df, filename="reading.parquet")

    assert uploaded == 2
    assert len(fake_s3.put_calls) == 2

    keys = [c["Key"] for c in fake_s3.put_calls]
    assert "alpha/prefix/2025/01/01/reading.parquet" in keys
    assert "alpha/prefix/2025/01/02/reading.parquet" in keys

    for call in fake_s3.put_calls:
        assert call["Bucket"] == "my-bucket"
        assert call["ContentType"] == "application/octet-stream"
        assert isinstance(call["Body"], (bytes, bytearray))
        assert len(call["Body"]) > 0


def test_upload_daily_partitions_handles_parquet_engine_error(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "my-bucket")

    fake_s3 = FakeS3()
    monkeypatch.setattr(m, "get_s3_client", lambda: fake_s3)

    monkeypatch.setattr(pd.DataFrame, "to_parquet", lambda *a,
                        **k: (_ for _ in ()).throw(Exception("boom")))

    daily_df = pd.DataFrame([{"day": "2025-01-01", "readings": 1}])

    with pytest.raises(RuntimeError) as excinfo:
        m.upload_daily_partitions_to_s3(daily_df)

    assert "Failed to write Parquet" in str(excinfo.value)


def test_truncate_table_executes_and_closes():
    conn = DummyConn()
    m.truncate_table(conn, table="alpha.recordings")

    assert conn.cursor_obj.executed == ["TRUNCATE TABLE alpha.recordings;"]
    assert conn.committed is True
    assert conn.closed is True


def test_run_summary_uses_provided_df_and_uploads(monkeypatch, readings_df):
    monkeypatch.setenv("S3_BUCKET", "my-bucket")
    fake_s3 = FakeS3()
    monkeypatch.setattr(m, "get_s3_client", lambda: fake_s3)

    summary, tables, uploaded = m.run_summary(
        df=readings_df, write_local_outputs=False)

    assert summary["rows"] == 3
    assert "daily" in tables
    assert uploaded == len(tables["daily"])
    assert len(fake_s3.put_calls) == uploaded


def test_lambda_handler_truncates_when_enabled(monkeypatch, readings_df):
    monkeypatch.setenv("S3_BUCKET", "my-bucket")

    monkeypatch.setattr(
        m,
        "run_summary",
        lambda **kwargs: (
            {"rows": 1, "timestamp_range": {"min": None, "max": None}},
            {"daily": pd.DataFrame([{"day": "2025-01-01", "readings": 1}])},
            1,
        ),
    )

    dummy_conn = DummyConn()
    monkeypatch.setattr(m, "get_sql_connection", lambda: dummy_conn)

    truncated = {"called": False}

    def fake_truncate(connection, table="alpha.recordings"):
        truncated["called"] = True

        connection.cursor().execute(f"TRUNCATE TABLE {table};")
        connection.commit()
        connection.close()

    monkeypatch.setattr(m, "truncate_table", fake_truncate)

    resp = m.lambda_handler(
        event={"truncate_after_upload": True,
               "table_name": "alpha.recordings", "s3_prefix": "x"},
        context=None,
    )

    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["days_uploaded"] == 1
    assert body["source_table"] == "alpha.recordings"
    assert truncated["called"] is True


def test_lambda_handler_skips_truncate_when_disabled(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "my-bucket")

    monkeypatch.setattr(
        m,
        "run_summary",
        lambda **kwargs: (
            {"rows": 1, "timestamp_range": {"min": None, "max": None}},
            {"daily": pd.DataFrame([{"day": "2025-01-01", "readings": 1}])},
            1,
        ),
    )

    monkeypatch.setattr(m, "get_sql_connection", lambda: (
        _ for _ in ()).throw(AssertionError("Should not connect")))

    resp = m.lambda_handler(
        event={"truncate_after_upload": False}, context=None)

    assert resp["statusCode"] == 200
