# drift_config.py

# Updated S3 paths
TRAIN_DATA_PATH = "s3://leadscoring-pipeline-data/training/train_reference.csv"
INFERENCE_DATA_PATH = "s3://leadscoring-pipeline-data/incoming/test2.csv"

# These remain dynamic (use templated Airflow date)
DRIFT_REPORT_PATH = "s3://leadscoring-pipeline-data/reports/drift_report_{{ ds }}.html"
DRIFT_FLAG_PATH = "s3://leadscoring-pipeline-data/flags/drift_flag_{{ ds }}.json"
