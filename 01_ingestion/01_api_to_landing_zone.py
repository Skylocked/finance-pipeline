# Import modules
import requests     # common attributes: status_code, text, headers // common methods: json(), get(), post(), put(), delete(), head(), raise_for_status()
import json         # common methods: dumps(), dump(), loads(), load() (with s means to string while no s means to file)
import os
from datetime import datetime


# Define our endpoints and storage paths
api_url = "https://open.er-api.com/v6/latest/USD"
volume_path = "/Volumes/dev_finance/raw/remittance"


# Print initial stdout
print(f"Starting pipeline...")
print(f"Calling API: {api_url}")

# Fetch the data from the API using .get() method from response package
# Outputs a response object
response = requests.get(api_url)


# Check if  API gave us a good response using .status_code method. 
if response.status_code == 200:
    
    # Convert the raw text response into a dict-type using .json() method from response package
    data = response.json()
    
    # Get current time to name our files and folders
    now = datetime.now()
    folder_date = now.strftime("%Y-%m-%d")                  # Example: 2026-07-03
    file_timestamp = now.strftime("%Y-%m-%d_%H%M%S")        # Example: 2026-07-03_212730
    
    # Build our Hive-style folder path and file name
    target_folder = f"{volume_path}/landing_date={folder_date}"
    target_file = f"{target_folder}/exchange_rates_{file_timestamp}.json"
    
    # Create the folder in our Volume if it doesn't exist yet
    os.makedirs(target_folder, exist_ok=True)       # exist_ok to prevent error if folder already exists
    
    # Write the data into the JSON file
    with open(target_file, "w") as file:    # open file with context manager in write mode
        json.dump(data, file)               # dump the data into the file
        
    print(f"Success! Data landed in: {target_file}")    # print stdout

else:
    # If the API is down, tell us exactly what error code it threw
    print(f"Failed! The API returned status code: {response.status_code}")