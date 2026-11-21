import dotenv
import os
from confluent_kafka import Producer    
import requests
import json 
import time
dotenv.load_dotenv(override=True)

API = os.getenv("API_URL")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9094")
DELAY = int(os.getenv("DELAY", "1"))


producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})


def callback(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")


def publish(postings):
    for post in postings:
        producer.produce(
            "raw-posts",
            key=str(post["id"]), 
            value=json.dumps(post, ensure_ascii=False),
            callback=callback
        )
        producer.poll(0)
    producer.flush()

def run():
    url = API 

    while url: 
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        postings = data.get("results", [])
        publish(postings)
        url = data.get("next")
        if url: 
            time.sleep(DELAY)

def main():
    while True:
        try:
            run()
            break
        except Exception as e:
            print(f"Error occurred: {e}. Retrying in 5 seconds...")
            time.sleep(5)

if __name__ == "__main__":
    main()
