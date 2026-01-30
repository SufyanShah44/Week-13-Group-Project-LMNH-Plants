import pandas as pd
import pytest
# pylint: skip-file


@pytest.fixture
def botanist_lookup():
    # what fetch_botanist_lookup(conn) should return
    return {"Alice": 101, "Bob": 202}


@pytest.fixture
def raw_df():
    # A small but representative input DataFrame
    return pd.DataFrame(
        [
            {
                "plant_id": "1",
                "botanist_name": "Alice",
                "recording_taken": "2025-01-01 10:00:00",
                "soil_moisture": "55.5",
                "temperature": "21.2",
                "last_watered": "2024-12-31",
            },
            {
                "plant_id": "2",
                "botanist_name": "Bob",
                "recording_taken": "bad-date",  # coerces to NaT
                "soil_moisture": "not-a-number",  # coerces to NaN -> row filtered out by .between
                "temperature": "18",
                "last_watered": "bad-date",  # coerces to NaT
            },
        ]
    )
