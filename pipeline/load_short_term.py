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


def seed_countries(connection: pyodbc.Connection, dataframe: pd.DataFrame) -> None:
    cur = connection.cursor()
    insert_sql = "INSERT INTO alpha.countries (country_name) VALUES (?)"

    rows = dataframe[["country_name"]].itertuples(index=False, name=None)

    cur.executemany(insert_sql, rows)
    connection.commit()
    cur.close()


def seed_origin_locations(connection: pyodbc.Connection, dataframe: pd.DataFrame) -> None:
    cur = connection.cursor()
    insert_sql = """
        INSERT INTO alpha.origin_locations (city, country_id, latitude, longitude)
        VALUES (?, ?, ?, ?)
    """
    rows = dataframe[["city", "country_id", "latitude", "longitude"]].itertuples(index=False, name=None)

    cur.executemany(insert_sql, rows)
    connection.commit()
    cur.close()


def seed_botanists(connection: pyodbc.Connection, dataframe: pd.DataFrame) -> None:
    cur = connection.cursor()
    insert_sql = """
        INSERT INTO alpha.botanists (name, email, phone)
        VALUES (?, ?, ?)
    """
    rows = dataframe[["botanist_name", "botanist_email",
                      "botanist_phone"]].itertuples(index=False, name=None)
    
    cur.executemany(insert_sql, rows)
    connection.commit()
    cur.close()


def seed_plants(connection: pyodbc.Connection, dataframe: pd.DataFrame) -> None:
    cur = connection.cursor()

    insert_sql = """
        INSERT INTO alpha.plants (plant_id, name, scientific_name, origin_location_id)
        VALUES (?, ?, ?, ?)
    """

    df = dataframe.loc[:, ["plant_id", "name",
                           "scientific_name", "origin_location_id"]].copy()

    df["plant_id"] = pd.to_numeric(df["plant_id"], errors="raise")  
    df["origin_location_id"] = pd.to_numeric(df["origin_location_id"], errors="coerce")

    df = df.where(pd.notnull(df), None)

    rows = []
    for plant_id, name, sci, origin_id in df.itertuples(index=False, name=None):
        rows.append((
            int(plant_id),
            None if name is None else str(name),
            None if sci is None else str(sci),
            None if origin_id is None else int(origin_id),
        ))

    cur.executemany(insert_sql, rows)
    connection.commit()
    cur.close()

def fetch_country_map(connection: pyodbc.Connection) -> pd.DataFrame:
    return pd.read_sql("SELECT country_id, country_name FROM alpha.countries", connection)


def fetch_origin_location_map(connection: pyodbc.Connection) -> pd.DataFrame:
    return pd.read_sql(
        "SELECT origin_location_id, city, country_id FROM alpha.origin_locations", connection)


def fetch_botanist_map(connection: pyodbc.Connection) -> pd.DataFrame:
    return pd.read_sql("SELECT botanist_id, email FROM alpha.botanists", connection)


def fetch_plant_map(connection: pyodbc.Connection) -> pd.DataFrame:
    return pd.read_sql(
        "SELECT plant_id, name, scientific_name, origin_location_id FROM alpha.plants", connection)

if __name__ == "__main__":
    load_dotenv()

    df = pd.read_csv("output.csv")
    df.columns = df.columns.str.strip()
    df = df.where(pd.notnull(df), None)

    conn = handler()

    try:
        #Countries
        seed_countries(conn, df[["country_name"]].drop_duplicates())
        df = df.merge(fetch_country_map(conn), on="country_name", how="left")

        #Origin locations
        df["country_id"] = pd.to_numeric(df["country_id"], errors="coerce")
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

        origin_df = df[["city", "country_id", "latitude", "longitude"]].drop_duplicates()
        origin_df = origin_df.where(pd.notnull(origin_df), None)

        seed_origin_locations(conn, origin_df)
        df = df.merge(fetch_origin_location_map(conn), on=["city", "country_id"], how="left")

        #Botanists
        if "botanist_phone" in df.columns:
            df["botanist_phone"] = df["botanist_phone"].astype("string")

        botanists_df = df[["botanist_name", "botanist_email", "botanist_phone"]].drop_duplicates()
        botanists_df = botanists_df.where(pd.notnull(botanists_df), None)

        seed_botanists(conn, botanists_df)
        df = df.merge(fetch_botanist_map(conn), left_on="botanist_email", right_on="email", how="left")

        #Plants
        plants_df = (
            df[["plant_id", "plant_name", "scientific_name", "origin_location_id"]]
            .drop_duplicates()
            .rename(columns={"plant_name": "name"})
        )
        seed_plants(conn, plants_df)

        if "plant_id" in df.columns:
            df = df.drop(columns=["plant_id"])

        plant_map = fetch_plant_map(conn)[["plant_id", "name", "scientific_name", "origin_location_id"]]

        df = df.merge(
            plant_map,
            left_on=["plant_name", "scientific_name", "origin_location_id"],
            right_on=["name", "scientific_name", "origin_location_id"],
            how="left",
        )

        if "plant_id" not in df.columns:
            for c in ["plant_id_db", "plant_id_x", "plant_id_y", "plant_id_plant"]:
                if c in df.columns:
                    df = df.rename(columns={c: "plant_id"})
                    break

    finally:
        conn.close()