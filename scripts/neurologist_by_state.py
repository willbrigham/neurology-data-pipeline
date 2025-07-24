'''
Author: Will Brigham

This script is to inspect the contents of the NPI API. It is a registry of all of the licensed medical professionals that is updated monthly.
I want to see how much information this API endpoint will provide me and I think it will be a good start to the neurology database.
'''

import requests
import json
import time
from pathlib import Path

# Exact taxonomy code for general neurology
NEUROLOGY_TAXONOMY_CODE = "2084N0400X"

# Major cities in Massachusetts (customize as needed)
MA_CITIES = [
    "Boston", "Worcester", "Springfield", "Cambridge", "Lowell",
    "Brockton", "Quincy", "Lynn", "Fall River", "Newton",
    "Lawrence", "Somerville", "Framingham", "Haverhill", "Waltham"
]

def is_primary_general_neurologist(provider):
    """Check if the provider is a primary general neurologist"""
    for taxonomy in provider.get("taxonomies", []):
        if taxonomy.get("code") == NEUROLOGY_TAXONOMY_CODE and taxonomy.get("primary") is True:
            return True
    return False

def fetch_by_city(state, city):
    """Fetch up to 1200 general neurologists in a city"""
    print(f"Fetching neurologists in {city}, {state}...")
    all_results = []
    base_url = "https://npiregistry.cms.hhs.gov/api/"
    for skip in range(0, 1200, 200):
        params = {
            "version": "2.1",
            "state": state,
            "city": city,
            "taxonomy_description": "Neurology",
            "enumeration_type": "NPI-1",
            "limit": 200,
            "skip": skip
        }
        res = requests.get(base_url, params=params)
        if res.status_code != 200:
            print(f"Error fetching {city}, skip={skip}: {res.status_code}")
            break
        batch = res.json().get("results", [])
        if not batch:
            break
        all_results.extend(batch)
        time.sleep(0.3)  # avoid hammering API

    # Filter for true neurologists only
    filtered = [p for p in all_results if is_primary_general_neurologist(p)]
    print(f"{len(filtered)} neurologists found in {city}")
    return filtered

def fetch_ri():
    """Fetch neurologists for Rhode Island in a single loop"""
    print("Fetching neurologists for RI...")
    all_results = []
    base_url = "https://npiregistry.cms.hhs.gov/api/"
    for skip in range(0, 1200, 200):
        params = {
            "version": "2.1",
            "state": "RI",
            "taxonomy_description": "Neurology",
            "enumeration_type": "NPI-1",
            "limit": 200,
            "skip": skip
        }
        res = requests.get(base_url, params=params)
        if res.status_code != 200:
            print(f"Error: {res.status_code}")
            break
        batch = res.json().get("results", [])
        if not batch:
            break
        all_results.extend(batch)
        time.sleep(0.3)

    filtered = [p for p in all_results if is_primary_general_neurologist(p)]
    print(f"{len(filtered)} neurologists found in RI")
    return filtered

def save_json(data, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved {len(data)} records to {path}")

if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[1]
    data_dir = project_root / "data" / "raw"

    # Fetch and save RI neurologists
    ri_data = fetch_ri()
    save_json(ri_data, data_dir / "neurologists_ri.json")

    # Fetch and save MA neurologists by city
    all_ma = []
    seen_npi_numbers = set()
    for city in MA_CITIES:
        city_results = fetch_by_city("MA", city)
        for provider in city_results:
            if provider["number"] not in seen_npi_numbers:
                all_ma.append(provider)
                seen_npi_numbers.add(provider["number"])

    save_json(all_ma, data_dir / "neurologists_ma.json")