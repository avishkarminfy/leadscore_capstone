import pandas as pd
import boto3
import json
import tempfile
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
from io import StringIO
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# S3 paths
TRAIN_DATA_PATH = "s3://leadscoring-pipeline-data/training/train_reference.csv"
INFERENCE_DATA_PATH = "s3://leadscoring-pipeline-data/incoming/test2.csv"

def read_csv_from_s3(s3_path):
    s3 = boto3.client("s3")
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj["Body"])
    return df

def upload_to_s3(content, bucket, key, is_json=False):
    s3 = boto3.client("s3")
    if is_json:
        content = json.dumps(content)
    elif isinstance(content, str):
        content = content.encode("utf-8")

    s3.put_object(Bucket=bucket, Key=key, Body=content)
    logging.info(f"Uploaded to s3://{bucket}/{key}")

def run_drift_report(execution_date):
    logging.info("Reading datasets from S3...")
    reference_df = read_csv_from_s3(TRAIN_DATA_PATH)
    current_df = read_csv_from_s3(INFERENCE_DATA_PATH)

    logging.info("Running data drift report...")
    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=reference_df, current_data=current_df)

    # Save and upload HTML report
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".html", delete=False) as tmpfile:
        report.save_html(tmpfile.name)
        tmpfile.seek(0)
        html_report = tmpfile.read()

    report_bucket = "leadscoring-pipeline-data"
    report_key = f"reports/drift_report_{execution_date}.html"
    upload_to_s3(html_report, report_bucket, report_key)

    # Extract drift result
    drift_result = report.as_dict()
    drift_detected = drift_result["metrics"][0]["result"]["dataset_drift"]

    # Upload JSON flag
    flag_bucket = "leadscoring-pipeline-data"
    flag_key = f"flags/drift_flag_{execution_date}.json"
    upload_to_s3({"drift_detected": drift_detected}, flag_bucket, flag_key, is_json=True)

    # Log alert
    if drift_detected:
        logging.warning("ALERT: Data Drift Detected!")
    else:
        logging.info("No Drift Detected.")

    return drift_detected

# For manual testing
if __name__ == "__main__":
    import datetime
    date_str = datetime.datetime.now().strftime("%Y-%m-%d")
    run_drift_report(execution_date=date_str)
