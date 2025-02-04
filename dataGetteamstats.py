import requests
import pandas as pd
import os
from dotenv import load_dotenv
import time
from datetime import datetime

def get_api_key():
    """Load API key from environment variable"""
    load_dotenv()
    api_key = os.getenv('API_KEY')
    if not api_key:
        raise ValueError("API_KEY not found in .env file")
    return api_key

def fetch_season_stats(year, api_key):
    """Fetch season statistics for a given year"""
    base_url = "https://api.collegefootballdata.com/stats/season"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "accept": "application/json"
    }
    
    params = {
        "year": year
    }
    
    try:
        response = requests.get(base_url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for year {year}: {str(e)}")
        return None

def main():
    # Create output directory if it doesn't exist
    output_dir = "output_directory"
    os.makedirs(output_dir, exist_ok=True)
    
    # Get API key
    api_key = get_api_key()
    
    # Define year range (2000 to current year)
    current_year = datetime.now().year
    years = range(2000, current_year + 1)
    
    # Initialize empty list to store all stats
    all_stats = []
    
    # Fetch data for each year
    for year in years:
        print(f"Fetching stats for {year}...")
        stats = fetch_season_stats(year, api_key)
        
        if stats:
            for stat in stats:
                stat['year'] = year
            all_stats.extend(stats)
            
        # Sleep to respect rate limits
        time.sleep(1)
    
    # Convert to DataFrame
    df = pd.DataFrame(all_stats)
    
    # Save to CSV
    output_file = os.path.join(output_dir, f"season_stats_{2000}_{current_year}.csv")
    df.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")

if __name__ == "__main__":
    main() 