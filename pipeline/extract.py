from flask import Flask
import requests

app = Flask(__name__)


def load_plant_data():
    plant_id = 1
    miss_limit = 5
    misses = 0

    while misses < miss_limit:
        try:
            plants = requests.get(
                f"https://tools.sigmalabs.co.uk/api/plants/{plant_id}")
        except requests.exceptions.RequestException:
            plant_id += 1
            continue

        if plants.status_code == 200:
            plants_json = plants.json()
            print(plants_json["plant_id"])
            misses = 0
        else:
            misses += 1

        plant_id += 1


if __name__ == "__main__":
    load_plant_data()
