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

# Base URLs for the endpoints
BASE_URL_GAMES = "https://api.collegefootballdata.com/games"
BASE_URL_STATS = "https://api.collegefootballdata.com/games/GetGamePlayerStats"

def get_auth_headers():
    """
    Return the headers for authentication using Bearer token.
    """
    return {"Authorization": f"Bearer {API_KEY}"}

def fetch_games_for_year(year):
    """
    Fetch games for a given year with classification 'fbs'.
    """
    params = {
        "year": year,
        "classification": "fbs"
    }
    try:
        response = requests.get(BASE_URL_GAMES, headers=get_auth_headers(), params=params, timeout=20)
        response.raise_for_status()
        games = response.json()
        return games
    except Exception as err:
        print(f"Error fetching games for year {year}: {err}")
    return None

def fetch_player_stats_for_game(game_id):
    """
    Fetch player stats for a specific game using its game_id.
    """
    params = {"gameId": game_id}
    try:
        response = requests.get(BASE_URL_STATS, headers=get_auth_headers(), params=params, timeout=20)
        response.raise_for_status()
        # Debug logging: print a snippet of the response content
        print(f"Fetched stats for game {game_id}: Status {response.status_code}")
        stats = response.json()
        return stats
    except Exception as err:
        print(f"Error fetching player stats for game {game_id}: {err}")
    return None

def main():
    all_stats = []

    # Loop through each year from 2000 to 2024 (inclusive)
    for year in range(2000, 2025):
        print(f"\nFetching games for year: {year}")
        games = fetch_games_for_year(year)
        if games:
            print(f"Retrieved {len(games)} games for {year}.")
            # For each game, get the game_id and fetch its player stats.
            for game in games:
                game_id = game.get("id")
                if not game_id:
                    print(f"No game ID found for a game record in {year}, skipping.")
                    continue
                stats = fetch_player_stats_for_game(game_id)
                if stats:
                    # If the API returns multiple records per game, iterate over them
                    # Tag each record with its season year and game_id
                    for stat in stats:
                        stat["year"] = year
                        stat["gameId"] = game_id
                        all_stats.append(stat)
                    print(f"Added {len(stats)} stats records for game {game_id}.")
                else:
                    print(f"No stats found for game {game_id}.")
                time.sleep(0.5)  # Brief pause to be polite to the API
        else:
            print(f"Failed to fetch games for year: {year}")
        time.sleep(1)  # Pause between years to avoid rate limiting

    if all_stats:
        print("\nSample game player stats record:")
        print(all_stats[0])
    else:
        print("No player stats data retrieved.")
        sys.exit(1)

    # Create output directory if it does not exist.
    output_dir = "output_directory"  # Change this as needed
    os.makedirs(output_dir, exist_ok=True)
    csv_file_path = os.path.join(output_dir, "game_player_stats_2000_2024.csv")
    
    # Determine CSV header by collecting all keys present in the records.
    headers = set()
    for stat in all_stats:
        headers.update(stat.keys())
    headers = list(headers)
    
    # Write the aggregated data to CSV.
    try:
        with open(csv_file_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for stat in all_stats:
                writer.writerow(stat)
        print(f"\nCSV file has been saved to: {csv_file_path}")
    except Exception as err:
        print(f"Error writing CSV file: {err}")

if __name__ == "__main__":
    main()
