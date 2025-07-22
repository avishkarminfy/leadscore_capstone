from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.dates import days_ago
from datetime import timedelta
import boto3
import json

# Configuration - UPDATE THESE VALUES
S3_BUCKET = 'leadscoring-pipeline-data'  # Your S3 bucket name
S3_INCOMING_PREFIX = 'processing/'  # Where new data arrives
DRIFT_FLAG_PREFIX = 'flags/'  # Where drift flags are stored

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['your-email@example.com']  # UPDATE: Add your email for alerts
}

def check_drift_flag(**context):
    """
    Check if data drift was detected by reading the drift flag from S3
    Returns 'trigger_retraining' if drift detected, 'skip_retraining' otherwise
    """
    s3 = boto3.client('s3')
    
    execution_date = context['ds']
    flag_key = f"{DRIFT_FLAG_PREFIX}drift_flag_{execution_date}.json"
    
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=flag_key)
        flag_data = json.loads(response['Body'].read().decode('utf-8'))
        
        drift_detected = flag_data.get('drift_detected', False)
        
        if drift_detected:
            print(f"Data drift detected! Flag value: {drift_detected}")
            return 'trigger_retraining'
        else:
            print("No data drift detected. Skipping retraining.")
            return 'skip_retraining'
            
    except Exception as e:
        print(f"Error reading drift flag: {str(e)}")
        return 'skip_retraining'

def dummy_task(**context):
    print("Retraining skipped - no drift detected")
    return True

with DAG(
    dag_id='master_pipeline_dag',
    default_args=default_args,
    description='Master pipeline orchestrating ETL, drift detection, and retraining',
    schedule_interval=None,  # Triggered by Lambda/S3 event or manually
    catchup=False,
    max_active_runs=1,
    tags=['master', 'orchestration', 'ml-pipeline'],
) as dag:

    # Step 1: Wait for new .csv file in S3
    wait_for_file = S3KeySensor(
        task_id='wait_for_new_file',
        bucket_name=S3_BUCKET,
        bucket_key=f'{S3_INCOMING_PREFIX}*.csv',
        wildcard_match=True,
        timeout=300,  # Wait up to 5 minutes
        poke_interval=30,
        mode='poke'
    )

    # Step 2: Trigger ETL DAG
    trigger_etl = TriggerDagRunOperator(
        task_id='trigger_etl_pipeline',
        trigger_dag_id='etl_s3_to_redshift_dag',
        wait_for_completion=True,
        poke_interval=60,
        conf={
            "triggered_by": "master_pipeline",
            "execution_date": "{{ ds }}"
        }
    )

    # Step 3: Trigger Data Drift Detection DAG
    trigger_drift_check = TriggerDagRunOperator(
        task_id='trigger_drift_detection',
        trigger_dag_id='data_drift_check_dag',
        wait_for_completion=True,
        poke_interval=60,
        conf={
            "triggered_by": "master_pipeline",
            "execution_date": "{{ ds }}"
        }
    )

    # Step 4: Check drift detection results
    check_drift = BranchPythonOperator(
        task_id='check_drift_results',
        python_callable=check_drift_flag,
        provide_context=True
    )

    # Step 5a: Trigger retraining if drift detected
    trigger_retraining = TriggerDagRunOperator(
        task_id='trigger_retraining',
        trigger_dag_id='retrain_model_simple_dag',
        wait_for_completion=True,
        poke_interval=60,
        conf={
            "triggered_by": "drift_detection",
            "execution_date": "{{ ds }}",
            "reason": "data_drift_detected"
        }
    )

    # Step 5b: Skip retraining if no drift
    skip_retraining = PythonOperator(
        task_id='skip_retraining',
        python_callable=dummy_task,
        provide_context=True
    )

    # DAG Task Flow
    wait_for_file >> trigger_etl >> trigger_drift_check >> check_drift
    check_drift >> [trigger_retraining, skip_retraining]
