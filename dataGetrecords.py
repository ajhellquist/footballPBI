import requests
import os
import sys
import csv
import time
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")

BASE_URL = "https://api.collegefootballdata.com/records"

def get_auth_headers():
    """
    Return the headers for authentication using Bearer token.
    """
    return {"Authorization": f"Bearer {API_KEY}"}

def ping_api():
    """
    Pings the API by making a minimal request (using a known year and week)
    to check connectivity.
    """
    test_year = 2020
    params = {
        "year": test_year
         # using week 1 as a minimal query
    }
    try:
        response = requests.get(BASE_URL, headers=get_auth_headers(), params=params, timeout=10)
        response.raise_for_status()
        print(f"Successfully connected to the API. Ping test for year {test_year} returned status code {response.status_code}.")
        return True
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error during API ping: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error during API ping: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error during API ping: {timeout_err}")
    except Exception as err:
        print(f"Unexpected error during API ping: {err}")
    return False

def fetch_records_for_year(year):
    """
    Fetch records data for a given year from the College Football Data API.
    """
    params = {"year": year}
    try:
        response = requests.get(BASE_URL, headers=get_auth_headers(), params=params, timeout=10)
        response.raise_for_status()
        print(f"Successfully fetched records for year {year}")
        return response.json()  # Return the actual data instead of True
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error during API request: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error during API request: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error during API request: {timeout_err}")
    except Exception as err:
        print(f"Unexpected error during API request: {err}")
    return None  # Return None instead of False on error

def main():
    # Ping the API first
    if not ping_api():
        sys.exit(1)
    
    all_records = []

    # Fetch records for the year's selected
    for year in range(2000, 2025):
        print(f"Fetching records for year: {year}")
        records = fetch_records_for_year(year)
        if records:  # This will handle both None and empty list cases
            all_records.extend(records)  # Use extend instead of append since records is a list
            print(f"Retrieved {len(records)} records for {year}")
        else:
            print(f"No records retrieved for {year}")
        time.sleep(1)

    # Print one record to check the data
    if all_records:
        print(all_records[0])

    output_dir = "output_directory"
    os.makedirs(output_dir, exist_ok=True)
    csv_file_path = os.path.join(output_dir, "records_2000_2024.csv")

    headers = set()
    for record in all_records:
        headers.update(record.keys())
    headers = list(headers)

    try:
        with open(csv_file_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for record in all_records:
                writer.writerow(record)
        print(f"CSV file has been saved to: {csv_file_path}")
    except Exception as err:
        print(f"Error writing CSV file: {err}")

if __name__ == "__main__":
    main()
