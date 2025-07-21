# data_processing/eda_sweetviz.py

import sweetviz as sv
import pandas as pd
from utils.s3_utils import upload_file_to_s3

def run_sweetviz_and_upload(df: pd.DataFrame,
                             output_html_path: str = "sweetviz_report.html",
                             bucket_name: str = "fake-bucket-lead-eda",
                             s3_key: str = "eda_reports/sweetviz_report.html"):
    """
    Generates a Sweetviz EDA report from the input DataFrame and uploads it to S3.
    
    Args:
        df (pd.DataFrame): The input DataFrame for EDA.
        output_html_path (str): Local path to save the HTML report.
        bucket_name (str): Target S3 bucket to upload the report.
        s3_key (str): Key name (path) in S3 where the file will be stored.
    """
    print("Running Sweetviz analysis...")
    report = sv.analyze(df)
    report.show_html(output_html_path)
    print(f"Report saved to {output_html_path}")
    
    upload_file_to_s3(local_path=output_html_path, bucket_name=bucket_name, s3_key=s3_key)
    print(f"Uploaded report to s3://{bucket_name}/{s3_key}")
