import json
import os
from pathlib import Path

import requests

# URL of the JSON data
url = "https://config-prod-lax.dexguru.biz/v1/inventories/amms/backend"

# Send HTTP request
response = requests.get(url)

# Load data in native Python format
data = response.json()

# Create the directory structure and files
for item in data:
    if item["type"] == "1inch":
        item["type"] = "one_inch"
    elif item["type"] == "pancakeswap_v3":
        item["type"] = "uniswap_v3"
    path = item["type"]
    if 'dodo' in item["type"]:
        path = 'dodo'
    folder_name = (
        Path(__file__).parent.parent / 'service' / 'dex' / path / 'deploys' / str(item['chain_id'])
    )
    os.makedirs(folder_name, exist_ok=True)
    with open(f"{folder_name}/metadata.json") as file:
        try:
            file_data = json.load(file)
        except json.decoder.JSONDecodeError:
            file_data = []

    data_to_add = {
        "name": item["name"],
        "type": item["type"],
        "enabled": item["enabled"],
        "contracts": item["contracts"],
    }
    if data_to_add in file_data:
        continue

    file_data.append(data_to_add)
    # Create file and write the data
    with open(f"{folder_name}/metadata.json", 'w') as file:
        json.dump(file_data, file)
