from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def move_processed_file(**context):
    """Move file from incoming to processing to avoid reprocessing"""
    s3 = boto3.client('s3')
    bucket = 'leadscoring-pipeline-data'
    
    # List files in incoming
    response = s3.list_objects_v2(Bucket=bucket, Prefix='incoming/')
    
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.csv'):
                # Move to processing folder
                new_key = obj['Key'].replace('incoming/', 'processing/')
                s3.copy_object(
                    Bucket=bucket,
                    CopySource={'Bucket': bucket, 'Key': obj['Key']},
                    Key=new_key
                )
                # Delete from incoming
                s3.delete_object(Bucket=bucket, Key=obj['Key'])
                print(f"Moved {obj['Key']} to {new_key}")
                
                # Store file info for pipeline
                context['task_instance'].xcom_push(
                    key='processed_file', 
                    value={'bucket': bucket, 'key': new_key}
                )
                break  # Process one file at a time

with DAG(
    'file_watcher_dag',
    default_args=default_args,
    description='Monitor S3 for new files and trigger pipeline',
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    catchup=False,
    max_active_runs=1,
    tags=['watcher', 's3-sensor']
) as dag:
    
    # Check for new CSV files
    check_for_files = S3KeySensor(
        task_id='check_for_new_files',
        bucket_name='leadscoring-pipeline-data',
        bucket_key='incoming/*.csv',
        wildcard_match=True,
        timeout=30,  # Stop checking after 30 seconds
        poke_interval=10,
        mode='poke',
        soft_fail=True  # Don't fail if no files found
    )
    
    # Move file to avoid reprocessing
    move_file = PythonOperator(
        task_id='move_file_to_processing',
        python_callable=move_processed_file,
        provide_context=True
    )
    
    # Trigger main pipeline
    trigger_pipeline = TriggerDagRunOperator(
        task_id='trigger_master_pipeline',
        trigger_dag_id='master_pipeline_dag',
        conf={
            "triggered_by": "file_watcher",
            "file_info": "{{ task_instance.xcom_pull(task_ids='move_file_to_processing', key='processed_file') }}"
        }
    )
    
    check_for_files >> move_file >> trigger_pipeline