import os
import boto3
from dotenv import load_dotenv

# 1. Load secrets from the .env file in the root directory
load_dotenv()

# 2. Initialize AWS S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv('AWS_SECRET_KEY')
)

bucket_name = os.getenv('S3_BUCKET_NAME', "aerospace-telemetry-raw")

# 3. Define the Lifecycle Rule aligned with our Cloud Ingester
lifecycle_policy = {
    'Rules': [
        {
            'ID': 'Archive-Raw-Bronze-Data-To-Glacier',
            'Filter': {
                'Prefix': 'bronze/' 
            },
            'Status': 'Enabled',
            'Transitions': [
                {
                    'Days': 30,
                    'StorageClass': 'GLACIER' 
                },
                {
                    'Days': 365,
                    'StorageClass': 'DEEP_ARCHIVE' 
                }
            ]
        }
    ]
}

def apply_lifecycle_policy():
    try:
        print(f"Applying Lifecycle Cost-Optimization to S3 bucket: {bucket_name}")
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_policy
        )
        print("Success! AWS will now monitor the 'bronze/' folder and move 30-day-old files to Glacier.")
    except Exception as e:
        print(f"Error applying policy: {e}")

if __name__ == "__main__":
    apply_lifecycle_policy()