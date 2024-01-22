import json

import requests

# Define the URL of the API endpoint
api_url = "https://config-prod-lax.dexguru.biz/v1/inventories/chains/backend"


# Function to fetch and process the data
def fetch_and_process_data(url):
    try:
        # Make a GET request to the API
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Extract data from response
        data = response.json()

        # Process data to match the desired structure
        processed_data = []
        for item in data:
            processed_item = {
                "id": item["id"],
                "name": item["name"],
                "type": item["type"],
                "enabled": item["enabled"],
                "native_token": item["native_token"],
                "stablecoin_addresses": [i.lower() for i in item["stablecoin_addresses"]],
            }
            processed_item["native_token"]["address"] = processed_item["native_token"][
                "address"
            ].lower()
            processed_data.append(processed_item)

        return processed_data

    except requests.RequestException as e:
        print(f"Error fetching data: {e}")


# Fetch and process the data
processed_data = fetch_and_process_data(api_url)

# Save the processed data to a JSON file
with open("../chains_config.json", "w") as file:
    json.dump(processed_data, file, indent=4)

print("Data saved to chains_config.json")
