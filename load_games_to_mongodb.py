import pandas as pd
from pymongo import MongoClient
import json
from ast import literal_eval
from dotenv import load_dotenv
import os
import sys
import certifi

def connect_to_mongodb():
    """Connect to MongoDB and return database object"""
    try:
        load_dotenv()  # Load environment variables from .env file
        connection_string = os.getenv('MONGO_URI')
        
        if not connection_string:
            raise ValueError("MONGO_URI not found in .env file")
            
        print("Attempting to connect to MongoDB...")
        client = MongoClient(connection_string, tlsCAFile=certifi.where())
        
        # Test the connection
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")
        
        db = client.cfb
        return db
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        sys.exit(1)

def process_game(row):
    """Process and convert game data types"""
    try:
        # Convert string lists to actual lists
        for field in ['away_line_scores', 'home_line_scores']:
            if isinstance(row[field], str):
                row[field] = literal_eval(row[field])
        
        # Convert numeric fields
        numeric_fields = [
            'away_id', 'home_id', 'year', 'home_points', 'away_points',
            'away_pregame_elo', 'away_postgame_elo', 'home_pregame_elo', 
            'home_postgame_elo', 'venue_id', 'excitement_index',
            'away_post_win_prob', 'home_post_win_prob'
        ]
        
        for field in numeric_fields:
            if pd.notna(row[field]):  # Check if value is not NaN
                if isinstance(row[field], str) and row[field].strip() == '':
                    row[field] = None
                elif field in ['away_post_win_prob', 'home_post_win_prob', 'excitement_index']:
                    row[field] = float(row[field]) if row[field] else None
                else:
                    row[field] = int(float(row[field])) if row[field] else None
        
        # Convert boolean fields
        bool_fields = ['conference_game', 'start_time_tbd', 'neutral_site', 'completed']
        for field in bool_fields:
            if isinstance(row[field], str):
                row[field] = row[field].lower() == 'true'
            
        return row
    except Exception as e:
        print(f"Error processing game: {row}")
        print(f"Error: {e}")
        return None

def load_games_to_mongodb(csv_path, db):
    """Load games from CSV to MongoDB"""
    try:
        # Check if file exists
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
            
        print(f"Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path)
        
        # Convert DataFrame to list of dictionaries
        games = df.to_dict('records')
        
        print(f"Processing {len(games)} games...")
        # Process each game
        processed_games = []
        for game in games:
            processed_game = process_game(game)
            if processed_game:
                processed_games.append(processed_game)
        
        # Create collection and insert records
        collection = db.games
        
        # Drop existing indexes if they exist
        collection.drop_indexes()
        
        print("Creating indexes...")
        # Create indexes for common queries
        collection.create_index([("year", 1)])
        collection.create_index([("home_id", 1), ("year", 1)])
        collection.create_index([("away_id", 1), ("year", 1)])
        collection.create_index([("conference", 1), ("year", 1)])
        
        print(f"Inserting {len(processed_games)} games...")
        # Insert games in bulk
        if processed_games:
            result = collection.insert_many(processed_games)
            return len(result.inserted_ids)
        return 0
    except Exception as e:
        print(f"Error loading games to MongoDB: {str(e)}")
        sys.exit(1)

def main():
    # Connect to MongoDB
    db = connect_to_mongodb()
    
    # Load games
    csv_path = "output_directory/games_2000_2024.csv"
    inserted_count = load_games_to_mongodb(csv_path, db)
    
    print(f"Successfully inserted {inserted_count} games into MongoDB")

if __name__ == "__main__":
    main() 