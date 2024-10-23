# Updated DbConnector.py
from pymongo import MongoClient
import os
import time

class DbConnector:
    def __init__(self, max_retries=30, retry_delay=2):
        self.uri = os.getenv('MONGODB_URI', 'mongodb://mongodb:27017/')
        self.database = os.getenv('MONGODB_DATABASE', 'geolife')
        
        print(f"Attempting to connect to MongoDB at {self.uri}")
        retry_count = 0
        last_error = None
        
        while retry_count < max_retries:
            try:
                self.client = MongoClient(
                    self.uri,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=5000,
                )
                # Test the connection
                self.client.server_info()
                self.db = self.client[self.database]
                print(f"Successfully connected to MongoDB database: {self.db.name}")
                return
            except Exception as e:
                last_error = e
                retry_count += 1
                print(f"Connection attempt {retry_count}/{max_retries} failed: {e}")
                if retry_count < max_retries:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
        
        raise Exception(f"Failed to connect to MongoDB after {max_retries} attempts. Last error: {last_error}")

    def close_connection(self):
        if hasattr(self, 'client'):
            self.client.close()
            print(f"Connection to {self.db.name}-db is closed")