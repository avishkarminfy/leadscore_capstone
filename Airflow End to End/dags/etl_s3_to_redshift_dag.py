from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import boto3
import os
import time  # Added for polling logic

# ===== CONFIGURATION - UPDATE THESE VALUES =====
GLUE_CRAWLER_NAME = 'leadscorecrawler'  # Your Glue crawler name
GLUE_JOB_NAME = 'etl_s3_to_redshift'    # Your Glue ETL job name
REGION = 'ap-south-1'                   # Your AWS region
S3_BUCKET = 'leadscoring-pipeline-data' # Your S3 bucket
S3_INCOMING_PREFIX = 'processing/'        # Where new data arrives
S3_PROCESSED_PREFIX = 'processed/'      # Where to move processed data
# =============================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def start_glue_crawler(**context):
    """Start the Glue crawler to catalog new data"""
    client = boto3.client('glue', region_name=REGION)
    
    # Get file info from DAG run conf if triggered by S3 event
    dag_run_conf = context.get('dag_run', {}).conf or {}
    s3_key = dag_run_conf.get('s3_key', '')
    
    print(f"Starting Glue crawler: {GLUE_CRAWLER_NAME}")
    if s3_key:
        print(f"Processing file: {s3_key}")
    
    response = client.start_crawler(Name=GLUE_CRAWLER_NAME)
    print(f"Crawler started successfully: {response}")
    
    # Custom polling logic (replace waiter)
    while True:
        crawler_state = client.get_crawler(Name=GLUE_CRAWLER_NAME)['Crawler']['State']
        print(f"Current crawler state: {crawler_state}")
        if crawler_state == 'READY':
            break
        time.sleep(10)  # Wait before polling again
    
    print("Crawler completed successfully")
    return response

def move_processed_file(**context):
    """Move processed file from incoming to processed folder"""
    s3 = boto3.client('s3', region_name=REGION)
    
    # Get file info from DAG run conf
    dag_run_conf = context.get('dag_run', {}).conf or {}
    s3_key = dag_run_conf.get('s3_key', '')
    
    if not s3_key:
        # If no specific file, get the latest file from incoming
        response = s3.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=S3_INCOMING_PREFIX
        )
        
        if 'Contents' in response:
            # Get the most recent file
            files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
            s3_key = files[0]['Key']
    
    if s3_key:
        # Generate new key in processed folder
        filename = os.path.basename(s3_key)
        execution_date = context['ds']
        new_key = f"{S3_PROCESSED_PREFIX}{execution_date}/{filename}"
        
        print(f"Moving file from {s3_key} to {new_key}")
        
        # Copy to processed folder
        s3.copy_object(
            Bucket=S3_BUCKET,
            CopySource={'Bucket': S3_BUCKET, 'Key': s3_key},
            Key=new_key
        )
        
        # Delete from incoming folder (optional - comment out if you want to keep original)
        # s3.delete_object(Bucket=S3_BUCKET, Key=s3_key)
        
        print(f"File moved successfully to {new_key}")
        
        # Store processed file path for downstream tasks
        context['task_instance'].xcom_push(key='processed_file_path', value=f"s3://{S3_BUCKET}/{new_key}")

with DAG(
    dag_id='etl_s3_to_redshift_dag',
    default_args=default_args,
    description='ETL pipeline from S3 to Redshift using AWS Glue',
    schedule_interval=None,  # Triggered by master DAG or S3 event
    catchup=False,
    tags=['etl', 'glue', 's3', 'redshift'],
) as dag:

    # Task 1: Run Glue Crawler to catalog new data
    run_crawler = PythonOperator(
        task_id='run_glue_crawler',
        python_callable=start_glue_crawler,
        provide_context=True,
    )

    run_glue_job = GlueJobOperator(
        task_id='run_glue_etl_job',
        job_name=GLUE_JOB_NAME,
        script_location=None,  # Job already exists in Glue
        region_name=REGION,
        iam_role_name=None,    # Use job's existing role
        create_job_kwargs={},  # Job already exists
        script_args={
            '--execution_date': '{{ ds }}',
            '--triggered_by': '{{ dag_run.conf.triggered_by if dag_run.conf else "scheduled" }}'
        }
    )
    # Task 3: Move processed file
    redshift_loading = PythonOperator(
        task_id='move_processed_file',
        python_callable=move_processed_file,
        provide_context=True,
    )

    # Define task dependencies
    run_crawler >> run_glue_job >> redshift_loading
