import os
import boto3
from dotenv import load_dotenv

# 1. Load AWS credentials from .env
load_dotenv()

# 2. Setup AWS S3 Client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
)

# 3. Use environment variables 
bucket_name = os.getenv('S3_BUCKET_NAME', "aerospace-telemetry-raw")
local_base_dir = "data/erp_export"
s3_base_prefix = "erp_export"

def upload_directory_to_s3():
    print(f"Starting Upload Agent for S3 bucket: {bucket_name}")
    
    upload_count = 0
    try:
        # Walk through the local directories
        for root, dirs, files in os.walk(local_base_dir):
            for file in files:
                if file.endswith('.json'):
                    # Get the full local path
                    local_path = os.path.join(root, file)
                    
                    # Extract the dimension folder name (e.g., 'dim_engines')
                    folder_name = os.path.basename(root)
                    
                    # Construct the S3 path
                    s3_path = f"{s3_base_prefix}/{folder_name}/{file}"
                    
                    print(f"Uploading {folder_name} -> s3://{bucket_name}/{s3_path}")
                    s3_client.upload_file(local_path, bucket_name, s3_path)
                    upload_count += 1
                    
        print(f"Upload Complete! Successfully transferred {upload_count} dimension files to S3.")
    except Exception as e:
        print(f"Upload Failed: {str(e)}")

if __name__ == "__main__":
    # Check if directory exists before trying to upload
    if not os.path.exists(local_base_dir):
        print(f"Error: Local directory '{local_base_dir}' not found. Run generate_dimensions.py first.")
    else:
        upload_directory_to_s3()