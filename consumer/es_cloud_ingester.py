import os
import json
import boto3
from confluent_kafka import Consumer
from dotenv import load_dotenv

# 1. Load secrets from the .env file in the root directory
load_dotenv()

#2. Initialize AWS S3 client using credentials from .env
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv('AWS_SECRET_KEY')
)

#3. Configure Kafka Consumer 
consumer_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVER'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
    'group.id': 's3_archiver_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe(['engine_telemetry_raw'])

def start_archiving():
    bucket_name = os.getenv('S3_BUCKET_NAME')
    print(f"Cloud Ingester active. Archiving to AWS S3 Bucket: {bucket_name}")

    try:
        while True:
            #Poll Kafka for new messages
            msg = consumer.poll(1.0)

            if msg is None: continue
            if msg.error():
                print(f"Kafka Error: {msg.error()}")
                continue

            #Decode the JSON payload
            record = json.loads(msg.value().decode('utf-8'))

            #Define S3 Partition path (e.g., bronze/engine_1/cycle_1.json)
            s3_path = f"bronze/engine_{record['engine_id']}/cycle_{record['cycle']}.json"

            #Put file into S3 bucket
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_path,
                Body=msg.value()
            )
            print(f"Archived to S3: {s3_path}")
    except KeyboardInterrupt:
        print("Stopping Ingester (User interrupted)...")
    except Exception as e:
        print(f"\nUnexpected Pipeline Error: {str(e)}")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_archiving()
