# data_ingestion/database_operations.py

import boto3

def save_to_s3(dataframe, bucket, key):
    s3 = boto3.client('s3')
    dataframe.to_csv(f'/tmp/{key}', index=False)
    s3.upload_file(f'/tmp/{key}', bucket, key)
