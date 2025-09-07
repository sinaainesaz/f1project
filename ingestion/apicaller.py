import pandas as pd
import requests
from typing import List, Dict, Any
import numpy as np
import os
import time

class APICaller:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def fetch_data(self, year: int, endpoint: str) -> pd.DataFrame:
        url = f"{self.base_url}/{year}/{endpoint.lstrip('/')}"

        try:
            response = requests.get(url)
            while response.status_code != 200:
                print(f"Waiting due to status code {response.status_code} for url: {url}")
                time.sleep(5)
                response = requests.get(url)
            data = response.json()

        except requests.RequestException as e:
            print(f"Request failed: {e}")

        endpoint_capitalized_with_s = endpoint.capitalize()  # Remove trailing 's' and capitalize
        endpoint_singular = endpoint.rstrip('s') 
        endpoint_capitalized = endpoint_singular.capitalize()  # Capitalize first letter
        print(f"Endpoint capitalized: {endpoint_capitalized}")
        table_name = f"{endpoint_capitalized}Table"
        print(f"Table name: {table_name}")
        #data_key = f"{endpoint_capitalized}s"  # Add 's' back for data key
        df = pd.json_normalize(data["MRData"][table_name][endpoint_capitalized_with_s])
        # let's explode the time column if it exists
        if "time" in df.columns:
            df = df.drop(columns=["time"])
        # and also if there are any columns after Circuit.Location.country, drop them too
        if "Circuit.Location.country" in df.columns:
            country_index = df.columns.get_loc("Circuit.Location.country")
            cols_to_drop = df.columns[country_index + 1:]
            df = df.drop(columns=cols_to_drop)

        # we should only normalize this column in json MRData.DriverTable.Drivers
        return df

if __name__ == "__main__":
    # Get the project root directory (parent of ingestion folder)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_dir = os.path.join(project_root, "data", "openf1source")
    
    # Create directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    with_year = ["races", "drivers"]
    api_caller = APICaller("https://api.jolpi.ca/ergast/f1/")
    
    for endpoint in with_year:
        year = 1950
        while year <= 2025:
            print(f"\nFetching data for: {endpoint} in year {year}")
            df = api_caller.fetch_data(year, endpoint)
            
            # we should append each year data in the same file without overwriting
            output_file = os.path.join(output_dir, f"{endpoint}_data.csv")
            df.to_csv(output_file, index=False, mode='a', header=not os.path.exists(output_file))
            print(f"Saved {endpoint} data for year {year} to: {output_file}")
            year += 1
