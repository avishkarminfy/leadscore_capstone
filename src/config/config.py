# config/config.py

import os

# AWS & Redshift
AWS_REGION = "ap-south-1"
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST") 
REDSHIFT_PORT = 5439
REDSHIFT_DB = "leadscoring"
REDSHIFT_USER = os.getenv("admin")
REDSHIFT_PASSWORD = os.getenv("Am7841043635")
REDSHIFT_TABLE = "public.leads_data"

# S3
S3_BUCKET = "leadscoringdata"
S3_TRAIN_PATH = "processed/train_data.csv"
S3_DRIFT_PATH = "drift/latest_batch.csv"

# MLflow
MLFLOW_TRACKING_URI = "https://183248601668.mlflow.sagemaker.ap-south-1.amazonaws.com"
MLFLOW_EXPERIMENT_NAME = "lead-conversion-classifier"

# Local fallback (for local runs only)
LOCAL_DATA_PATH = "data/leads.csv"
