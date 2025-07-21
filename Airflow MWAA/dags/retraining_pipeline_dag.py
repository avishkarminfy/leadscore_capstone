from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta, timezone
import boto3
import os
import subprocess
import json

# ---- Config ----
S3_BUCKET = 'airflowdags11'
NEW_DATA_PREFIX = 'new_data/'
SCRIPTS_PREFIX = 'scripts/'
RETRAIN_WINDOW_DAYS = 30

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}


# Get last 1-month CSV files from S3
def get_monthly_data_keys(**kwargs):
    s3 = boto3.client('s3')
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=RETRAIN_WINDOW_DAYS)
    print(f"Filtering files modified after: {cutoff_date}")

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=NEW_DATA_PREFIX)
    csv_files = [
        obj['Key'] for obj in response.get('Contents', [])
        if obj['Key'].endswith('.csv') and obj['LastModified'] >= cutoff_date
    ]

    if not csv_files:
        raise ValueError("No recent CSV files found for retraining.")

    print(f"Found {len(csv_files)} files for retraining.")
    kwargs['ti'].xcom_push(key='monthly_file_keys', value=csv_files)


# ⬇ Download & Run a Python script from S3 with args
def run_script_from_s3(script_name, args=None):
    s3 = boto3.client('s3')
    local_path = f"/tmp/{script_name}"

    try:
        s3.download_file(S3_BUCKET, f"{SCRIPTS_PREFIX}{script_name}", local_path)
        print(f"Downloaded {script_name}")
    except Exception as e:
        raise Exception(f"Failed to download {script_name}: {e}")

    env = os.environ.copy()
    env['PYTHONPATH'] = '/tmp:' + env.get('PYTHONPATH', '')

    cmd = ["python3", local_path]
    if args:
        cmd.extend(args)

    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    print("STDOUT:\n", result.stdout)
    if result.stderr:
        print("STDERR:\n", result.stderr)
    if result.returncode != 0:
        raise Exception(f"Script {script_name} failed.")


# Preprocess recent data
def preprocess(**kwargs):
    file_keys = kwargs['ti'].xcom_pull(task_ids='get_recent_csv_files', key='monthly_file_keys')
    if not file_keys:
        raise ValueError("No file keys received for preprocessing.")
    
    args = [
        "--s3_bucket", S3_BUCKET,
        "--s3_keys", json.dumps(file_keys)
    ]
    run_script_from_s3("preprocess.py", args)


# Train model on preprocessed data
def train_model():
    run_script_from_s3("train.py")


# DAG Definition
with DAG(
    dag_id="lead_scoring_monthly_retraining",
    default_args=default_args,
    description="Monthly retraining pipeline for lead scoring using last 30 days of data",
    schedule_interval="@monthly",  # Run on 1st of every month
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["lead_scoring", "mlops", "monthly"],
) as dag:

    # Optional: Sensor for file presence (disable if you want pure schedule-based run)
    wait_for_file = S3KeySensor(
        task_id="wait_for_any_recent_file",
        bucket_name=S3_BUCKET,
        bucket_key=NEW_DATA_PREFIX + "*.csv",
        wildcard_match=True,
        aws_conn_id="aws_default",
        timeout=600,
        poke_interval=60,
        mode="poke"
    )

    # Get recent file keys (last 30 days)
    get_recent_csv_files = PythonOperator(
        task_id="get_recent_csv_files",
        python_callable=get_monthly_data_keys,
    )

    # Preprocess data
    preprocess_data = PythonOperator(
        task_id="preprocess_recent_data",
        python_callable=preprocess,
    )

    # Train model
    retrain_model = PythonOperator(
        task_id="train_model_on_recent_data",
        python_callable=train_model,
    )

    # DAG Flow
    wait_for_file >> get_recent_csv_files >> preprocess_data >> retrain_model
