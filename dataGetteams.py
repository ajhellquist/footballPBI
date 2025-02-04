import requests
import os
import sys
import csv
import time
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")

BASE_URL = "https://api.collegefootballdata.com/teams"

def get_auth_headers():
    """
    Return the headers for authentication using Bearer token.
    """
    return {"Authorization": f"Bearer {API_KEY}"}

def ping_api():
    """
    Pings the API by making a minimal request to check connectivity.
    """
    try:
        response = requests.get(BASE_URL, headers=get_auth_headers(), timeout=10)
        response.raise_for_status()
        print(f"Successfully connected to the API. Ping test returned status code {response.status_code}.")
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

def fetch_teams():
    """
    Fetch teams data from the College Football Data API.
    """
    try:
        response = requests.get(BASE_URL, headers=get_auth_headers(), timeout=10)
        response.raise_for_status()
        print("Successfully fetched teams data")
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error during API request: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error during API request: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error during API request: {timeout_err}")
    except Exception as err:
        print(f"Unexpected error during API request: {err}")
    return None

def main():
    # Ping the API first
    if not ping_api():
        sys.exit(1)
    
    # Fetch teams data
    teams = fetch_teams()
    if not teams:
        print("No teams data retrieved")
        sys.exit(1)
    
    print(f"Retrieved {len(teams)} teams")

    # Print one record to check the data
    if teams:
        print(teams[0])

    output_dir = "output_directory"
    os.makedirs(output_dir, exist_ok=True)
    csv_file_path = os.path.join(output_dir, "teams.csv")

    # Get all possible headers from the data
    headers = set()
    for team in teams:
        headers.update(team.keys())
    headers = list(headers)

    try:
        with open(csv_file_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for team in teams:
                writer.writerow(team)
        print(f"CSV file has been saved to: {csv_file_path}")
    except Exception as err:
        print(f"Error writing CSV file: {err}")

if __name__ == "__main__":
    main() 