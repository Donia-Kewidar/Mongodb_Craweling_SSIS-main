import json
import requests
from pymongo import MongoClient

# MongoDB Database and Collection names
DB_NAME = "participants_db"
COLLECTION_NAME = "participants_data"

# API URL
URL = "https://api.egytech.fyi/participants"

# Header information to mimic a browser request
HEADERS = {
    'accept': 'application/json'
}

def get_participants(url):
    """Fetches participant data from a given URL."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=20)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        data = response.json()  # Directly parse the JSON response
        return data['results']
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def process_participants(participants):
    """Processes and structures participant data."""
    return [
        {
            'participant_Date': participant['Timestamp'],
            'Gender': participant['Gender'],
            'Degree': participant['Degree'],
            'BusinessMarket': participant['BusinessMarket'],
            'Title': participant['Title'],
            'ProgrammingLanguages': participant['ProgrammingLanguages'],
            'BusinessSize': participant['BusinessSize'],
            'Yoe': participant['Yoe'],
            'YoeBuckets': participant['YoeBuckets'],
            'BusinessFocus': participant['BusinessFocus'],
            'TotalCompensationEgp': participant['TotalCompensationEgp'],
            'BusinessLine': participant['BusinessLine'],
            'TotalCompensationEgpBuckets': participant['TotalCompensationEgpBuckets'],
            'Industries': participant['Industries'],
            'WorkSetting': participant['WorkSetting'],
            'Level': participant['Level'],
            'IsEgp': participant['IsEgp'],
            'CompanyLocation': participant['CompanyLocation']
        }
        for participant in participants
    ] if participants else []

def save_to_mongodb(data, db_name, collection_name):
    """Saves data to a MongoDB collection."""
    if data:
        try:
            client = MongoClient('mongodb://localhost:27017/')
            db = client[db_name]
            collection = db[collection_name]
            collection.insert_many(data)
            print(f"Successfully inserted {len(data)} documents into MongoDB.")
        except Exception as e:
            print(f"Error saving data to MongoDB: {e}")
        finally:
            client.close()
    else:
        print("No data to save.")

def main():
    # Fetch participants data from the API
    participants = get_participants(URL)
    if participants is not None:
        # Process participants into structured data
        processed_data = process_participants(participants)
        # Save the processed data to MongoDB

        save_to_mongodb(processed_data, 'Jobs_database' , 'participants')

if __name__ == "__main__":
    main()
