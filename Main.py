import requests
import pymongo
import time
import datetime
import random

def query_api(endpoint, headers, duration, collection):
    end_time = datetime.datetime.now() + datetime.timedelta(minutes=duration)
    while datetime.datetime.now() < end_time:
        try:
            rando = random.random()
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()  # Raises an HTTPError if the response was an error response
            data = response.json()
            collection.insert_one(data)
            print(f"Data stored at {datetime.datetime.now()}: {data}")
            time.sleep(rando)  # Query every minute; adjust as needed
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
        except pymongo.errors.PyMongoError as e:
            print(f"MongoDB error: {e}")

# Example usage
api_endpoint = "https://httpbin.org/get"
api_key = "your_api_key"
x_minutes = 300  # Duration in minutes

headers = {
    "Authorization": f"Bearer {api_key}"
}

if __name__ == "__main__":

    atlas_user = 'steveman'
    atlas_password = 'needham1993'
    atlas_cluster = 'cluster0.xzwyxwj.mongodb.net'

    database = 'MAIN'
    col = 'MAIN' 

    client = pymongo.MongoClient(f"mongodb+srv://{atlas_user}:{atlas_password}@{atlas_cluster}")

    db = client[database]
    co = db[col]

    query_api(api_endpoint, headers, x_minutes, co)
