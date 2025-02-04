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

def process_stat(row):
    """Process stat data and convert types as needed"""
    try:
        # Convert to proper types
        row['year'] = int(row['year'])
        row['statValue'] = float(row['statValue'])
        
        return row
    except Exception as e:
        print(f"Error processing row: {row}")
        print(f"Error: {e}")
        return None

def load_stats_to_mongodb(csv_path, db):
    """Load season stats from CSV to MongoDB"""
    try:
        # Check if file exists
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
            
        print(f"Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path)
        
        # Convert DataFrame to list of dictionaries
        stats = df.to_dict('records')
        
        print(f"Processing {len(stats)} statistics...")
        # Process each stat
        processed_stats = []
        for stat in stats:
            processed_stat = process_stat(stat)
            if processed_stat:
                processed_stats.append(processed_stat)
        
        # Create collection and insert stats
        collection = db.teamstats 
        
        # Drop existing indexes if they exist
        collection.drop_indexes()
        
        print("Creating indexes...")
        # Create indexes for common queries
        collection.create_index([("year", 1)])  # Index on year
        collection.create_index([("team", 1)])  # Index on team
        collection.create_index([("category", 1)])  # Index on category
        
        print(f"Inserting {len(processed_stats)} statistics...")
        # Insert stats in bulk
        if processed_stats:
            result = collection.insert_many(processed_stats)
            return len(result.inserted_ids)
        return 0
    except Exception as e:
        print(f"Error loading stats to MongoDB: {str(e)}")
        sys.exit(1)

def main():
    # Connect to MongoDB
    db = connect_to_mongodb()
    
    # Load stats
    csv_path = "output_directory/season_stats_2000_2024.csv"
    inserted_count = load_stats_to_mongodb(csv_path, db)
    
    print(f"Successfully inserted {inserted_count} statistics into MongoDB")

if __name__ == "__main__":
    main() 