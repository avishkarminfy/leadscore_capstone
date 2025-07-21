# monitoring/drift_detector.py

from evidently.report import Report
from evidently.metrics import DataDriftPreset
import pandas as pd
import boto3
from config.config import *

def load_s3_csv(path):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=path)
    return pd.read_csv(obj['Body'])

def generate_drift_report():
    train_df = load_s3_csv(S3_TRAIN_PATH)
    new_df = load_s3_csv(S3_DRIFT_PATH)

    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=train_df, current_data=new_df)
    report.save_html("drift_report.html")
