# utils/s3_model_loader.py

import boto3
import joblib
import os

def load_model_from_s3(bucket_name='mlflowartifacts12',
                       key='artifacts/logistic_regression_model.pkl',
                       local_path='model.pkl'):
    if not os.path.exists(local_path):  # Avoid downloading again
        s3 = boto3.client('s3')
        s3.download_file(bucket_name, key, local_path)
    model = joblib.load(local_path)
    return model
