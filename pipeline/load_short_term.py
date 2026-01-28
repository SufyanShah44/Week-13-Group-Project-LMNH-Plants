from os import environ as ENV
import pandas as pd 

from dotenv import load_dotenv
import pyodbc


def handler(event=None, context=None):
    conn_str = (f"DRIVER={{{ENV['DB_DRIVER']}}};SERVER={ENV['DB_HOST']};"
                f"PORT={ENV['DB_PORT']};DATABASE={ENV['DB_NAME']};"
                f"UID={ENV['DB_USERNAME']};PWD={ENV['DB_PASSWORD']};Encrypt=no;")

    conn = pyodbc.connect(conn_str)

    return conn


def insert_recordings(connection: pyodbc.Connection, df: pd.DataFrame) -> None:
    cur = connection.cursor()

    insert_sql = """
        INSERT INTO alpha.recordings (
            plant_id,
            botanist_id,
            [timestamp],
            soil_moisture,
            temperature,
            last_watered
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """

    rows = df[
        ["plant_id", "botanist_id", "timestamp",
            "soil_moisture", "temperature", "last_watered"]
    ].itertuples(index=False, name=None)

    cur.executemany(insert_sql, rows)
    connection.commit()
    cur.close()

if __name__ == "__main__":
    load_dotenv()

    conn = handler()

    insert_recordings(conn, df)
