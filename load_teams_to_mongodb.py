import pandas as pd
from pymongo import MongoClient
import json
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
        # Use certifi's certificate bundle for SSL verification
        client = MongoClient(connection_string, tlsCAFile=certifi.where())
        
        # Test the connection
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")
        
        db = client.cfb
        return db
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        sys.exit(1)

def process_team(row):
    """Process team data and convert types as needed"""
    try:
        # Convert to proper types
        row['id'] = int(row['id'])
        
        return row
    except Exception as e:
        print(f"Error processing row: {row}")
        print(f"Error: {e}")
        return None

def load_teams_to_mongodb(csv_path, db):
    """Load teams from CSV to MongoDB"""
    try:
        # Check if file exists
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
            
        print(f"Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path)
        
        # Convert DataFrame to list of dictionaries
        teams = df.to_dict('records')
        
        print(f"Processing {len(teams)} teams...")
        # Process each team
        processed_teams = []
        for team in teams:
            processed_team = process_team(team)
            if processed_team:
                processed_teams.append(processed_team)
        
        # Create collection and insert teams
        collection = db.teams
        
        # Drop existing indexes if they exist
        collection.drop_indexes()
        
        print("Creating indexes...")
        # Create indexes for common queries
        collection.create_index([("id", 1)])  # Index on team ID
        collection.create_index([("conference", 1)])  # Index on conference
        
        print(f"Inserting {len(processed_teams)} teams...")
        # Insert teams in bulk
        if processed_teams:
            result = collection.insert_many(processed_teams)
            return len(result.inserted_ids)
        return 0
    except Exception as e:
        print(f"Error loading teams to MongoDB: {str(e)}")
        sys.exit(1)

def main():
    # Connect to MongoDB
    db = connect_to_mongodb()
    
    # Load teams
    csv_path = "output_directory/teams.csv"
    inserted_count = load_teams_to_mongodb(csv_path, db)
    
    print(f"Successfully inserted {inserted_count} teams into MongoDB")

if __name__ == "__main__":
    main() 