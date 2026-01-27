"""Extract script to read plant health information from an API"""

import requests
import pandas as pd


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


def load_plant_data():
    plant_id = 1
    miss_limit = 5
    misses = 0
    rows = []

    while misses < miss_limit:
        try:
            r = requests.get(
                f"https://tools.sigmalabs.co.uk/api/plants/{plant_id}")
            data = r.json()
        except requests.exceptions.RequestException:
            misses += 1
            plant_id += 1
            continue

        row = extract_fields(data)

        if row["plant_name"]:
            rows.append(row)
            misses = 0
        else:
            misses += 1

        plant_id += 1

    return pd.DataFrame(rows)


if __name__ == "__main__":
    load_plant_data()
