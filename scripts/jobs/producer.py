import csv
import json
import time
import random
from kafka import KafkaProducer

# Configuration
INPUT_FILE = '/local_data/Tweets.csv'  # Path inside the container
TOPIC_NAME = 'tweets_topic'
BOOTSTRAP_SERVERS = ['kafka:9092']

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def main():
    # 1. Initialize Producer
    print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=json_serializer
    )

    # 2. Read CSV and Publish
    print(f"Reading from {INPUT_FILE}...")
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            count = 0
            for row in reader:
                # Send row to Kafka
                producer.send(TOPIC_NAME, row)
                
                count += 1
                if count % 100 == 0:
                    print(f"Sent {count} tweets...")

                # Simulate real-time delay (random sleep between 0.01s and 0.1s)
                time.sleep(random.uniform(0.01, 0.1))
                
            print(f"Finished! Sent {count} total tweets.")
            producer.flush()
            
    except FileNotFoundError:
        print(f"Error: Could not find {INPUT_FILE}. Make sure it is mounted.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()