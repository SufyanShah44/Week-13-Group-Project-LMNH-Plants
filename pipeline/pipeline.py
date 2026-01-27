"""Single pipeline script to run ETL functions to get data and store in DB"""

import asyncio

from extract import load_plant_data

if __name__ == "__main__":
    plant_data_df = asyncio.run(load_plant_data())
