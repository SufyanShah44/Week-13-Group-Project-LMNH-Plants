"""Fixtures for test file"""

import pandas as pd
import pytest


@pytest.fixture
def readings_df():
    # Intentionally mixes strings + invalids to exercise coercion
    return pd.DataFrame(
        [
            {
                "recording_id": "1",
                "plant_id": "10",
                "botanist_id": "100",
                "timestamp": "2025-01-01 10:00:00",
                "soil_moisture": "50.5",
                "temperature": "21.0",
                "last_watered": "2024-12-31 10:00:00",
            },
            {
                "recording_id": "2",
                "plant_id": "10",
                "botanist_id": "101",
                "timestamp": "2025-01-01 11:00:00",
                "soil_moisture": "55.0",
                "temperature": "20.0",
                # makes negative days_since_last_watered
                "last_watered": "2025-01-01 12:00:00",
            },
            {
                "recording_id": "3",
                "plant_id": "11",
                "botanist_id": "100",
                "timestamp": "bad-date",              # NaT
                "soil_moisture": "not-a-number",      # NaN
                "temperature": "18.0",
                "last_watered": "bad-date",           # NaT
            },
        ]
    )
