import pandas as pd
import boto3
import json
import tempfile
import os
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
from io import StringIO
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# ===== CONFIGURATION - UPDATE THESE PATHS =====
S3_BUCKET = "leadscoring-pipeline-data"                              # Your S3 bucket
TRAIN_DATA_PATH = f"s3://{S3_BUCKET}/training/train_reference.csv"  # Reference training data
INFERENCE_DATA_PREFIX = f"s3://{S3_BUCKET}/processed/"              # Where processed data is stored
REPORT_PREFIX = "reports/"                                           # Where to save drift reports
FLAG_PREFIX = "flags/"                                               # Where to save drift flags
# ============================================

def get_latest_inference_data(s3_bucket, prefix, execution_date=None):
    """Get the latest inference data file from S3"""
    s3 = boto3.client('s3')
    
    # If execution date provided, look for files from that date
    if execution_date:
        prefix = f"{prefix}{execution_date}/"
    
    response = s3.list_objects_v2(
        Bucket=s3_bucket,
        Prefix=prefix
    )
    
    if 'Contents' not in response:
        raise ValueError(f"No files found in s3://{s3_bucket}/{prefix}")
    
    # Get the most recent CSV file
    csv_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.csv')]
    if not csv_files:
        raise ValueError(f"No CSV files found in s3://{s3_bucket}/{prefix}")
    
    latest_file = sorted(csv_files, key=lambda x: x['LastModified'], reverse=True)[0]
    
    return f"s3://{s3_bucket}/{latest_file['Key']}"

def read_csv_from_s3(s3_path):
    """Read CSV file from S3"""
    s3 = boto3.client("s3")
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    
    logging.info(f"Reading file from s3://{bucket}/{key}")
    
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(obj["Body"])
        logging.info(f"Successfully read {len(df)} rows from {s3_path}")
        return df
    except Exception as e:
        logging.error(f"Error reading file from S3: {str(e)}")
        raise

def upload_to_s3(content, bucket, key, is_json=False):
    """Upload content to S3"""
    s3 = boto3.client("s3")
    
    if is_json:
        content = json.dumps(content, indent=2)
    elif isinstance(content, str):
        content = content.encode("utf-8")

    s3.put_object(Bucket=bucket, Key=key, Body=content)
    logging.info(f"Uploaded to s3://{bucket}/{key}")

def run_drift_report(execution_date=None):
    """Run drift detection and generate reports"""
    
    # Use provided execution date or current date
    if not execution_date:
        execution_date = os.environ.get('EXECUTION_DATE', datetime.now().strftime("%Y-%m-%d"))
    
    logging.info(f"Running drift detection for execution date: {execution_date}")
    
    try:
        # Read reference training data
        logging.info("Reading reference training data...")
        reference_df = read_csv_from_s3(TRAIN_DATA_PATH)
        
        # Get the latest inference data
        logging.info("Looking for latest inference data...")
        inference_data_path = get_latest_inference_data(
            S3_BUCKET, 
            "processed/", 
            execution_date
        )
        
        logging.info(f"Reading inference data from: {inference_data_path}")
        current_df = read_csv_from_s3(inference_data_path)
        
        # Ensure both dataframes have the same columns
        common_columns = list(set(reference_df.columns) & set(current_df.columns))
        reference_df = reference_df[common_columns]
        current_df = current_df[common_columns]
        
        logging.info(f"Comparing {len(common_columns)} common columns")
        
        # Run drift detection
        logging.info("Running Evidently data drift report...")
        report = Report(metrics=[DataDriftPreset()])
        report.run(reference_data=reference_df, current_data=current_df)
        
        # Save HTML report
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".html", delete=False) as tmpfile:
            report.save_html(tmpfile.name)
            tmpfile.seek(0)
            html_report = tmpfile.read()
        
        # Upload HTML report to S3
        report_key = f"{REPORT_PREFIX}drift_report_{execution_date}.html"
        upload_to_s3(html_report, S3_BUCKET, report_key)
        
        # Extract drift results
        drift_result = report.as_dict()
        drift_detected = drift_result["metrics"][0]["result"]["dataset_drift"]
        
        # Create detailed drift summary
        drift_summary = {
            "drift_detected": drift_detected,
            "execution_date": execution_date,
            "reference_data": TRAIN_DATA_PATH,
            "current_data": inference_data_path,
            "report_location": f"s3://{S3_BUCKET}/{report_key}",
            "timestamp": datetime.now().isoformat(),
            "columns_analyzed": len(common_columns),
            "reference_rows": len(reference_df),
            "current_rows": len(current_df)
        }
        
        # Add column-level drift information if available
        if "metrics" in drift_result and len(drift_result["metrics"]) > 0:
            column_drifts = {}
            for metric in drift_result["metrics"]:
                if "result" in metric and "drift_by_columns" in metric["result"]:
                    column_drifts = metric["result"]["drift_by_columns"]
                    drift_summary["column_drifts"] = column_drifts
                    
                    # Count drifted columns
                    drifted_columns = [col for col, drift in column_drifts.items() if drift]
                    drift_summary["drifted_columns_count"] = len(drifted_columns)
                    drift_summary["drifted_columns"] = drifted_columns
        
        # Upload drift flag/summary to S3
        flag_key = f"{FLAG_PREFIX}drift_flag_{execution_date}.json"
        upload_to_s3(drift_summary, S3_BUCKET, flag_key, is_json=True)
        
        # Log results
        if drift_detected:
            logging.warning("⚠️  ALERT: Data Drift Detected!")
            if "drifted_columns" in drift_summary:
                logging.warning(f"Drifted columns ({len(drift_summary['drifted_columns'])}): {', '.join(drift_summary['drifted_columns'])}")
        else:
            logging.info("✅ No Data Drift Detected.")
        
        logging.info(f"Drift detection completed. Report saved to: s3://{S3_BUCKET}/{report_key}")
        
        return drift_detected
        
    except Exception as e:
        logging.error(f"Error during drift detection: {str(e)}")
        
        # Create error flag
        error_summary = {
            "drift_detected": False,
            "execution_date": execution_date,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }
        
        flag_key = f"{FLAG_PREFIX}drift_flag_{execution_date}.json"
        upload_to_s3(error_summary, S3_BUCKET, flag_key, is_json=True)
        
        raise

# For manual testing
if __name__ == "__main__":
    import sys
    
    # Accept execution date as command line argument
    execution_date = None
    if len(sys.argv) > 1:
        execution_date = sys.argv[1]
    
    run_drift_report(execution_date=execution_date)