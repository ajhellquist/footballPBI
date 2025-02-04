#!/usr/bin/env python3
import os
import sys
import csv
import time
import requests
from dotenv import load_dotenv

load_dotenv()

# Retrieve your API key from the environment
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    print("Error: API_KEY is not set. Please check your .env file.")
    sys.exit(1)

# Base URL for the getGames endpoint
BASE_URL = "https://api.collegefootballdata.com/games"

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
        "year": test_year,
        "week": 1  # using week 1 as a minimal query
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

def fetch_games_for_year(year):
    """
    Fetch game data for a given year from the College Football Data API.
    """
    params = {"year": year}
    try:
        response = requests.get(BASE_URL, headers=get_auth_headers(), params=params, timeout=20)
        response.raise_for_status()
        games = response.json()
        return games
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error for year {year}: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error for year {year}: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error for year {year}: {timeout_err}")
    except Exception as err:
        print(f"Error for year {year}: {err}")
    return None

def main():
    # Ping the API first
    if not ping_api():
        print("Failed to connect to the API. Please check your network connection and API key.")
        sys.exit(1)
    
    all_games = []
    
    # Loop through each year from 2000 to 2024 (inclusive)
    for year in range(2000, 2025):
        print(f"Fetching games for year: {year}")
        games = fetch_games_for_year(year)
        if games:
            for game in games:
                game["year"] = year
                all_games.append(game)
            print(f"Retrieved {len(games)} games for {year}")
        else:
            print(f"Failed to fetch games for year: {year}")
        time.sleep(1)  # Pause to avoid hitting API rate limits
    
    # Print one game to check the data
    print(all_games[0])

    # Set the output directory (modify this path as needed)
    output_dir = "output_directory"  # Change this to your desired directory
    os.makedirs(output_dir, exist_ok=True)
    csv_file_path = os.path.join(output_dir, "games_2000_2024.csv")
    
    # Determine the CSV header by collecting all keys present in the game objects.
    headers = set()
    for game in all_games:
        headers.update(game.keys())
    headers = list(headers)
    
    # Write the aggregated game data to CSV
    try:
        with open(csv_file_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for game in all_games:
                writer.writerow(game)
        print(f"CSV file has been saved to: {csv_file_path}")
    except Exception as err:
        print(f"Error writing CSV file: {err}")

if __name__ == "__main__":
    main()