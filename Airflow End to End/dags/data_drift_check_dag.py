from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import boto3
import json
import os

# ===== CONFIGURATION - UPDATE THESE VALUES =====
S3_BUCKET = 'leadscoring-pipeline-data'           # Your S3 bucket
DRIFT_SCRIPT_PATH = '/usr/local/airflow/dags/scripts/detect_drift.py'  # Path to drift detection script
REGION = 'ap-south-1'                             # Your AWS region
# =============================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_drift_detection(**context):
    """Execute drift detection script and return results"""
    import subprocess
    import sys

    execution_date = context['ds']

    print(f"Running drift detection for date: {execution_date}")

    # Run the drift detection script
    cmd = [sys.executable, DRIFT_SCRIPT_PATH]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            env={
                **os.environ,
                'EXECUTION_DATE': execution_date
            }
        )

        print("Drift detection output:")
        print(result.stdout)

        if result.stderr:
            print("Drift detection errors:")
            print(result.stderr)

        # Read the drift flag from S3 to get the result
        s3 = boto3.client('s3', region_name=REGION)
        flag_key = f"flags/drift_flag_{execution_date}.json"

        response = s3.get_object(Bucket=S3_BUCKET, Key=flag_key)
        flag_data = json.loads(response['Body'].read().decode('utf-8'))
        drift_detected = flag_data.get('drift_detected', False)

        # Push drift result to XCom for other tasks
        context['task_instance'].xcom_push(key='drift_detected', value=drift_detected)
        context['task_instance'].xcom_push(key='drift_report_path', value=f"s3://{S3_BUCKET}/reports/drift_report_{execution_date}.html")

        return drift_detected

    except subprocess.CalledProcessError as e:
        print(f"Drift detection failed with error: {e}")
        print(f"Error output: {e.stderr}")
        raise

def prepare_alert_message(**context):
    """Prepare alert message if drift is detected"""
    drift_detected = context['task_instance'].xcom_pull(task_ids='run_drift_detection', key='drift_detected')
    drift_report_path = context['task_instance'].xcom_pull(task_ids='run_drift_detection', key='drift_report_path')

    if drift_detected:
        message = {
            "default": "Data drift detected in lead scoring pipeline!",
            "email": f"""
Data Drift Alert - Lead Scoring Pipeline

Alert Time: {context['ts']}
Execution Date: {context['ds']}

Data drift has been detected in the incoming data compared to the training reference data.

Drift Report: {drift_report_path}

Actions:
- Model retraining will be triggered automatically
- Review the drift report for detailed analysis
- Monitor model performance closely

This is an automated alert from the Lead Scoring ML Pipeline.
            """
        }

        return json.dumps(message)

    return None

with DAG(
    dag_id='data_drift_check_dag',
    default_args=default_args,
    description='Check data drift between training and new data',
    schedule_interval=None,  # Triggered by master DAG
    catchup=False,
    tags=['drift-detection', 'monitoring', 'ml-pipeline'],
) as dag:

    # Task 1: Run drift detection
    detect_drift = PythonOperator(
        task_id='run_drift_detection',
        python_callable=run_drift_detection,
        provide_context=True,
    )

    # Task 2: Prepare alert message (optional output, no SNS)
    prepare_alert = PythonOperator(
        task_id='prepare_alert_message',
        python_callable=prepare_alert_message,
        provide_context=True,
    )

    # Define task dependencies
    detect_drift >> prepare_alert
