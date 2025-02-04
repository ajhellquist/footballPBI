import pandas as pd
from pymongo import MongoClient
import json
from ast import literal_eval
from dotenv import load_dotenv
import os
import sys
import certifi  # Add this import

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

def process_record(row):
    """Convert string representations of dictionaries to actual dictionaries"""
    try:
        # Convert string representations of dictionaries to actual dictionaries
        for field in ['conferenceGames', 'homeGames', 'awayGames', 'total']:
            if isinstance(row[field], str):
                row[field] = literal_eval(row[field])
        
        # Convert to proper types
        row['year'] = int(row['year'])
        row['teamId'] = int(row['teamId'])
        row['expectedWins'] = float(row['expectedWins'])
        
        return row
    except Exception as e:
        print(f"Error processing row: {row}")
        print(f"Error: {e}")
        return None

def load_records_to_mongodb(csv_path, db):
    """Load records from CSV to MongoDB"""
    try:
        # Check if file exists
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
            
        print(f"Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path)
        
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        
        print(f"Processing {len(records)} records...")
        # Process each record
        processed_records = []
        for record in records:
            processed_record = process_record(record)
            if processed_record:
                processed_records.append(processed_record)
        
        # Create collection and insert records
        collection = db.records
        
        # Drop existing indexes if they exist
        collection.drop_indexes()
        
        print("Creating indexes...")
        # Create indexes for common queries
        collection.create_index([("year", 1), ("teamId", 1)])
        collection.create_index([("conference", 1), ("year", 1)])
        
        print(f"Inserting {len(processed_records)} records...")
        # Insert records in bulk
        if processed_records:
            result = collection.insert_many(processed_records)
            return len(result.inserted_ids)
        return 0
    except Exception as e:
        print(f"Error loading records to MongoDB: {str(e)}")
        sys.exit(1)

def main():
    # Connect to MongoDB
    db = connect_to_mongodb()
    
    # Load records
    csv_path = "output_directory/records_2000_2024.csv"
    inserted_count = load_records_to_mongodb(csv_path, db)
    
    print(f"Successfully inserted {inserted_count} records into MongoDB")

if __name__ == "__main__":
    main() 