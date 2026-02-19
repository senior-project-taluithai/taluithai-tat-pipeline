#!/usr/bin/env python3
import json
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from pathlib import Path

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

# MongoDB connection details
USERNAME = os.environ.get("MONGODB_USERNAME")
PASSWORD = os.environ.get("MONGODB_PASSWORD")
IP_ADDRESS = os.environ.get("MONGODB_HOST", "34.87.52.21")
PORT = os.environ.get("MONGODB_PORT", "27017")
DATABASE_NAME = "google-scrape"

# Connection string
CONNECTION_STRING = f"mongodb://{USERNAME}:{PASSWORD}@{IP_ADDRESS}:{PORT}/?authSource=admin"

def import_json_to_mongodb(file_path, collection_name):
    """Import JSON file to MongoDB collection"""
    print(f"\n{'='*60}")
    print(f"Processing: {file_path}")
    print(f"Collection: {collection_name}")
    
    try:
        # Read JSON file
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Ensure data is a list
        if not isinstance(data, list):
            data = [data]
        
        total_records = len(data)
        print(f"Total records to import: {total_records}")
        
        if total_records == 0:
            print("No records to import. Skipping...")
            return
        
        # Connect to MongoDB
        client = MongoClient(CONNECTION_STRING)
        db = client[DATABASE_NAME]
        collection = db[collection_name]
        
        # Insert documents in batches
        batch_size = 1000
        inserted_count = 0
        
        for i in range(0, total_records, batch_size):
            batch = data[i:i + batch_size]
            try:
                result = collection.insert_many(batch, ordered=False)
                inserted_count += len(result.inserted_ids)
                print(f"Progress: {inserted_count}/{total_records} records inserted")
            except BulkWriteError as bwe:
                # Handle duplicate key errors
                inserted_count += bwe.details.get('nInserted', 0)
                print(f"Batch insert completed with some duplicates. Inserted: {inserted_count}/{total_records}")
        
        print(f"✓ Successfully imported {inserted_count} records to collection '{collection_name}'")
        
        # Close connection
        client.close()
        
    except Exception as e:
        print(f"✗ Error importing {file_path}: {str(e)}")
        raise

def main():
    """Main function to import all JSON files"""
    print("="*60)
    print("MongoDB Import Script")
    print("="*60)
    print(f"Database: {DATABASE_NAME}")
    print(f"Server: {IP_ADDRESS}:{PORT}")
    
    # Get current directory
    current_dir = Path(__file__).parent
    
    # Find all JSON files starting with 'thailand-'
    json_files = list(current_dir.glob('thailand-*.json'))
    
    if not json_files:
        print("\nNo JSON files found matching pattern 'thailand-*.json'")
        return
    
    print(f"\nFound {len(json_files)} JSON files to import")
    
    # Import each file
    for json_file in sorted(json_files):
        # Extract collection name from filename
        # e.g., 'thailand-attraction.json' -> 'attraction'
        collection_name = json_file.stem.replace('thailand-', '')
        
        try:
            import_json_to_mongodb(json_file, collection_name)
        except Exception as e:
            print(f"Failed to import {json_file.name}. Continuing with next file...")
            continue
    
    print("\n" + "="*60)
    print("Import process completed!")
    print("="*60)

if __name__ == "__main__":
    main()
