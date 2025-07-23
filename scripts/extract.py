'''
Author: Will Brigham

This script is to inspect the contents of the NPI API. It is a registry of all of the licensed medical professionals that is updated monthly.
I want to see how much information this API endpoint will provide me and I think it will be a good start to the neurology database.
'''

import requests
import json
from pathlib import Path

# Get the root directory regardless of where this script is run from
project_root = Path(__file__).resolve().parents[1]
data_dir = project_root/"data"/"raw"
data_dir.mkdir(parents=True, exist_ok=True)

# Fetch and save sample data
params = {
    'version': '2.1',
    'limit': 20,
    'taxonomy_description': 'Neurology',
    'state': 'RI'
}
response = requests.get('https://npiregistry.cms.hhs.gov/api/', params=params)
data = response.json()

with open(data_dir/"neurologists_sample.json", "w") as f:
    json.dump(data, f, indent=2)

print(f"Data saved to {data_dir/'neurologists_sample.json'}")