import os
import json
import time
import pandas as pd
from confluent_kafka import Producer
from dotenv import load_dotenv

# 1. Load secrets from the .env file in the root directory
load_dotenv()

# 2. Configure the Kafka connection securely
kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
}

# 3. Initialize the Kafka Producer
producer = Producer(kafka_config)
topic_name = "engine_telemetry_raw"

# 4. Delivery callback to confirm data reached the cloud
def delivery_report(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Engine {msg.key().decode()} -> Kafka Partition: [{msg.partition()}]")

def stream_engine_data():
    file_path = "data/raw/train_FD001.txt"

    # We now map the operational settings AND the first 10 physical sensors
    cols = [
        'engine_id', 'cycle', 'setting1', 'setting2', 'setting3',
        's1', 's2', 's3', 's4', 's5', 's6', 's7', 's8', 's9', 's10'
    ]
    
    try:
        # Tell Pandas to pull the first 15 columns
        df = pd.read_csv(file_path, sep=r'\s+', header=None, names=cols, usecols=range(15))
    except FileNotFoundError:
        print(f"Error: Could not find data at {file_path}. Please ensure Engine files are extracted there.")
        return

    print("--- Starting FULL Jet Engine Simulation (15 Data Points) ---")

    try:
        for index, row in df.iterrows():
            # Convert the 15-column row into a dictionary and then JSON
            payload = row.to_dict()
            message_value = json.dumps(payload)
            
            # Send to Kafka, keeping engine_id as the partition key
            producer.produce(
                topic_name,
                key=str(int(payload['engine_id'])), # Ensure key is a clean integer string
                value=message_value,
                callback=delivery_report
            )
            
            # Flush every 10 records to keep latency low
            if index % 10 == 0:
                producer.flush()

            # Slow down to simulate real-time sensor readings (2 pings per second)
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nStreaming stopped by user.")
    except Exception as e:
        print(f"\nUnexpected error occured: {str(e)}")
    finally:
        producer.flush()

if __name__ == "__main__":
    stream_engine_data()