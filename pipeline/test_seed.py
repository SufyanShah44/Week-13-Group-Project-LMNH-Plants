import pandas as pd
from unittest.mock import MagicMock
import seed as m


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


def test_seed_countries_runs_executemany():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur

    df = pd.DataFrame({"country_name": ["UK"]})

    m.seed_countries(conn, df)

    cur.executemany.assert_called_once()
    conn.commit.assert_called_once()


def test_seed_origin_locations_runs_executemany():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur

    df = pd.DataFrame({
        "city": ["London"],
        "country_id": [1],
        "latitude": [51.5],
        "longitude": [-0.1],
    })

    m.seed_origin_locations(conn, df)

    cur.executemany.assert_called_once()
    conn.commit.assert_called_once()


def test_seed_botanists_runs_executemany():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur

    df = pd.DataFrame({
        "botanist_name": ["Alice"],
        "botanist_email": ["a@test.com"],
        "botanist_phone": ["123"],
    })

    m.seed_botanists(conn, df)

    cur.executemany.assert_called_once()
    conn.commit.assert_called_once()


def test_seed_plants_runs_executemany():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur

    df = pd.DataFrame({
        "plant_id": ["1"],
        "name": ["Aloe"],
        "scientific_name": ["Aloe vera"],
        "origin_location_id": ["10"],
    })

    m.seed_plants(conn, df)

    cur.executemany.assert_called_once()
    conn.commit.assert_called_once()


def test_fetch_country_map_calls_read_sql(monkeypatch):
    mock_df = pd.DataFrame({"country_id": [1], "country_name": ["UK"]})
    monkeypatch.setattr(m.pd, "read_sql", MagicMock(return_value=mock_df))

    conn = MagicMock()
    df = m.fetch_country_map(conn)

    assert not df.empty


def test_fetch_origin_location_map_calls_read_sql(monkeypatch):
    monkeypatch.setattr(m.pd, "read_sql", MagicMock(
        return_value=pd.DataFrame({"id": [1]})))

    conn = MagicMock()
    df = m.fetch_origin_location_map(conn)

    assert not df.empty


def test_fetch_botanist_map_calls_read_sql(monkeypatch):
    monkeypatch.setattr(m.pd, "read_sql", MagicMock(
        return_value=pd.DataFrame({"id": [1]})))

    conn = MagicMock()
    df = m.fetch_botanist_map(conn)

    assert not df.empty


def test_fetch_plant_map_calls_read_sql(monkeypatch):
    monkeypatch.setattr(m.pd, "read_sql", MagicMock(
        return_value=pd.DataFrame({"id": [1]})))

    conn = MagicMock()
    df = m.fetch_plant_map(conn)

    assert not df.empty
