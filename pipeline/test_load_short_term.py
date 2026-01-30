# test_recordings_script.py

import pandas as pd
from unittest.mock import MagicMock
import load_short_term as m


def test_handler_returns_connection(monkeypatch):
    monkeypatch.setattr(m, "ENV", {
        "DB_DRIVER": "driver",
        "DB_HOST": "host",
        "DB_PORT": "1234",
        "DB_NAME": "db",
        "DB_USERNAME": "user",
        "DB_PASSWORD": "pass",
    })

    mock_connect = MagicMock(return_value="CONN")
    monkeypatch.setattr(m.pyodbc, "connect", mock_connect)

    conn = m.handler()

    assert conn == "CONN"


def test_insert_recordings_runs_executemany():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur

    df = pd.DataFrame({
        "plant_id": [1],
        "botanist_id": [2],
        "timestamp": ["2024-01-01 00:00:00"],
        "soil_moisture": [55.5],
        "temperature": [21.3],
        "last_watered": ["2024-01-01 00:00:00"],
    })

    m.insert_recordings(conn, df)

    cur.executemany.assert_called_once()
    conn.commit.assert_called_once()
    cur.close.assert_called_once()
