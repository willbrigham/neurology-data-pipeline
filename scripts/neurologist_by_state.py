'''
Author: Will Brigham

This script is to inspect the contents of the NPI API. It is a registry of all of the licensed medical professionals that is updated monthly.
I want to see how much information this API endpoint will provide me and I think it will be a good start to the neurology database.
'''

import requests
import json
from pathlib import Path
import time

def fetch_neurologists(out_path):
    base_url = "https://npiregistry.cms.hhs.gov/api/"
    params = {
    'version': '2.1',
    'enumeration_type': 'NPI-1',
    'taxonomy_description': 'Neurology',
    'state' : 'MA',
    'limit': 200
    }
    all_results = []
    skip = 0

    while True:
        params["skip"] = skip
        res = requests.get(base_url, params=params)
        if res.status_code != 200:
            print(f"Error: {res.status_code}")
            break
        data = res.json()
        results = data.get("results", [])
        if not results:
            break

        all_results.extend(results)
        skip += 200
        print(f"Retrieved {len(results)} more... (total: {len(all_results)})")
        time.sleep(0.5)  # Avoid hammering the API

    # Save to file
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(all_results, f, indent=2)

    print(f"Saved {len(all_results)} providers to {out_path}")


if __name__ == "__main__":
    # Resolve path from root, not relative to script
    project_root = Path(__file__).resolve().parents[1]
    raw_data_dir = project_root/"data"/"raw"

    #fetch_neurologists("RI", raw_data_dir / "neurologists_ri.json")
    fetch_neurologists(raw_data_dir/"neurologists.json")