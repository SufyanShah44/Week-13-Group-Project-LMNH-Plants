"""Single pipeline script to run ETL functions to get data and store in DB"""

import asyncio
from dotenv import load_dotenv

from extract import load_plant_data
from transform import transform_readings
from load_short_term import handler, insert_recordings


def main():
    plant_data_df = asyncio.run(load_plant_data())
    recordings_data = transform_readings(plant_data_df)
    conn = handler()
    insert_recordings(conn, recordings_data)


if __name__ == "__main__":
    load_dotenv()
    main()
