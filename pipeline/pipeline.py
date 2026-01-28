"""Single pipeline script to run ETL functions to get data and store in DB"""

import asyncio
from dotenv import load_dotenv

from extract import load_plant_data
from transform import transform_readings
from load_short_term import handler as get_db_connection, insert_recordings


def main():
    plant_data_df = asyncio.run(load_plant_data())
    recordings_data = transform_readings(plant_data_df)
    conn = get_db_connection()
    insert_recordings(conn, recordings_data)


def handler(event, context):
    load_dotenv()
    main()
    return {
        "statusCode": 200,
        "body": "ETL pipeline completed successfully"
    }


if __name__ == "__main__":
    load_dotenv()
    main()
