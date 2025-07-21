# utils/s3_utils.py

import boto3
import os

def upload_file_to_s3(local_path: str,
                      bucket_name: str,
                      s3_key: str):
    """
    Uploads a local file to a specified S3 bucket.

    Args:
        local_path (str): Path to the local file.
        bucket_name (str): S3 bucket name.
        s3_key (str): Key name (file path) in the bucket.
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"❌ File {local_path} does not exist.")

    s3 = boto3.client("s3")
    s3.upload_file(Filename=local_path, Bucket=bucket_name, Key=s3_key)
