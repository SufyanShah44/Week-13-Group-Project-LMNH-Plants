"""Test file for transform script"""
# pylint: skip-file
import numpy as np
import pandas as pd
import pytest

import transform as t


class DummyConn:
    """Stand-in to satisfy .close() calls."""

    def close(self) -> None:
        return None


@pytest.fixture
def mock_db(monkeypatch, botanist_lookup):
    """
    Mock real DB access
    """
    monkeypatch.setattr(t, "get_sql_connection", lambda: DummyConn())
    monkeypatch.setattr(t, "fetch_botanist_lookup",
                        lambda conn: botanist_lookup)


def test_transform_happy_path_maps_botanist_and_selects_columns(mock_db, raw_df):
    out = t.transform_readings(raw_df)

    assert list(out.columns) == [
        "plant_id",
        "botanist_id",
        "timestamp",
        "soil_moisture",
        "temperature",
        "last_watered",
    ]

    # Only first row should survive because second has soil_moisture -> NaN
    assert len(out) == 1

    row = out.iloc[0]
    assert row["plant_id"] == 1
    assert row["botanist_id"] == 101
    assert pd.Timestamp("2025-01-01 10:00:00") == row["timestamp"]
    assert row["soil_moisture"] == 55.5
    assert row["temperature"] == 21.2
    assert pd.Timestamp("2024-12-31") == row["last_watered"]


def test_transform_raises_when_botanist_missing(monkeypatch, raw_df):
    # Mock DB
    monkeypatch.setattr(t, "get_sql_connection", lambda: DummyConn())
    monkeypatch.setattr(t, "fetch_botanist_lookup",
                        lambda conn: {"SomeoneElse": 1})

    with pytest.raises(ValueError) as excinfo:
        t.transform_readings(raw_df)

    msg = str(excinfo.value)
    assert "Missing botanist IDs for:" in msg
    # Should mention at least one missing botanist
    assert "Alice" in msg or "Bob" in msg


def test_transform_filters_out_of_range_values(mock_db):
    df = pd.DataFrame(
        [
            {
                "plant_id": "1",
                "botanist_name": "Alice",
                "recording_taken": "2025-01-01",
                "soil_moisture": "0",
                "temperature": "0",
                "last_watered": "2025-01-01",
            },
            {
                "plant_id": "2",
                "botanist_name": "Alice",
                "recording_taken": "2025-01-01",
                "soil_moisture": "100",
                "temperature": "40",
                "last_watered": "2025-01-01",
            },
            {
                "plant_id": "3",
                "botanist_name": "Alice",
                "recording_taken": "2025-01-01",
                "soil_moisture": "-1",   # out of range
                "temperature": "20",
                "last_watered": "2025-01-01",
            },
            {
                "plant_id": "4",
                "botanist_name": "Alice",
                "recording_taken": "2025-01-01",
                "soil_moisture": "50",
                "temperature": "41",     # out of range
                "last_watered": "2025-01-01",
            },
        ]
    )

    out = t.transform_readings(df)

    assert out["plant_id"].tolist() == [1, 2]


def test_transform_enforces_int64_dtypes(mock_db):
    df = pd.DataFrame(
        [
            {
                "plant_id": 7,
                "botanist_name": "Bob",
                "recording_taken": "2025-01-01",
                "soil_moisture": "10",
                "temperature": "20",
                "last_watered": "2025-01-01",
            }
        ]
    )

    out = t.transform_readings(df)

    assert out["plant_id"].dtype == np.int64
    assert out["botanist_id"].dtype == np.int64


def test_transform_datetime_coercion_keeps_nat_when_invalid(mock_db):
    df = pd.DataFrame(
        [
            {
                "plant_id": "1",
                "botanist_name": "Alice",
                "recording_taken": "not-a-date",
                "soil_moisture": "50",
                "temperature": "20",
                "last_watered": "also-bad",
            }
        ]
    )

    out = t.transform_readings(df)

    assert len(out) == 1
    assert pd.isna(out.loc[0, "timestamp"])
    assert pd.isna(out.loc[0, "last_watered"])


def test_transform_does_not_mutate_input_df(mock_db, raw_df):
    raw_before = raw_df.copy(deep=True)
    _ = t.transform_readings(raw_df)

    pd.testing.assert_frame_equal(raw_df, raw_before)
