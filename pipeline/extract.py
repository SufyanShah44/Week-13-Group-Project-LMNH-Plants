"""Extract script to read plant health information from an API"""

import pandas as pd
import asyncio
import aiohttp


def extract_fields(plant):
    return {
        "plant_id": plant.get("plant_id"),
        "plant_name": plant.get("name"),
        "scientific_name": plant.get("scientific_name"),
        "last_watered": plant.get("last_watered"),
        "recording_taken": plant.get("recording_taken"),
        "soil_moisture": plant.get("soil_moisture"),
        "temperature": plant.get("temperature"),

        "botanist_name": plant.get("botanist", {}).get("name"),
        "botanist_email": plant.get("botanist", {}).get("email"),
        "botanist_phone": plant.get("botanist", {}).get("phone"),

        "city": plant.get("origin_location", {}).get("city"),
        "country_name": plant.get("origin_location", {}).get("country"),
        "latitude": plant.get("origin_location", {}).get("latitude"),
        "longitude": plant.get("origin_location", {}).get("longitude"),
    }


async def fetch_plant(session, plant_id):
    """Fetch a single plant's data"""
    url = f"https://tools.sigmalabs.co.uk/api/plants/{plant_id}"
    try:
        async with session.get(url) as response:
            return await response.json()
    except Exception:
        return None


async def load_plant_data():
    """Load plant data concurrently"""
    plant_id = 1
    miss_limit = 5
    consecutive_misses = 0
    rows = []

    async with aiohttp.ClientSession() as session:
        while consecutive_misses < miss_limit:
            tasks = [fetch_plant(session, plant_id + i) for i in range(10)]
            results = await asyncio.gather(*tasks)

            for i, data in enumerate(results):
                if data and data.get("name"):
                    row = extract_fields(data)
                    rows.append(row)
                    consecutive_misses = 0
                else:
                    consecutive_misses += 1

                    if consecutive_misses >= miss_limit:
                        break

            plant_id += 10

    return pd.DataFrame(rows)


if __name__ == "__main__":
    asyncio.run(load_plant_data())
