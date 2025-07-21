from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import boto3
import time

# Configuration
BUCKET_NAME = 'newdata232' 
S3_PREFIX = 'incoming/'  
GLUE_CRAWLER_NAME = 'leadscore'
GLUE_JOB_NAME = 'lead_data_etl_job'
REGION = 'ap-south-1'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def start_glue_crawler():
    client = boto3.client('glue', region_name=REGION)
    response = client.start_crawler(Name=GLUE_CRAWLER_NAME)
    print("Crawler started:", response)
    return response

def wait_for_crawler():
    client = boto3.client('glue', region_name=REGION)
    while True:
        response = client.get_crawler(Name=GLUE_CRAWLER_NAME)
        status = response['Crawler']['State']
        print(f"Crawler status: {status}")
        if status == 'READY':
            break
        time.sleep(15)  # wait before checking again
    return "Crawler completed"

with DAG(
    dag_id='etl_s3_triggered_dag',
    default_args=default_args,
    schedule_interval=None,  # Triggered only on new file
    catchup=False,
    tags=['s3', 'glue', 'redshift', 'trigger'],
) as dag:

    # Sensor: Wait for new file in S3
    wait_for_new_file = S3KeySensor(
        task_id='wait_for_new_file_in_s3',
        bucket_name=BUCKET_NAME,
        bucket_key=S3_PREFIX + '*',  # wildcard match
        wildcard_match=True,
        timeout=600,  # max wait time
        poke_interval=60,
        aws_conn_id='aws_default',
        mode='poke',
    )

    run_crawler = PythonOperator(
        task_id='run_glue_crawler',
        python_callable=start_glue_crawler,
    )

    wait_crawler_complete = PythonOperator(
        task_id='wait_for_crawler_to_complete',
        python_callable=wait_for_crawler,
    )

    run_glue_job = GlueJobOperator(
        task_id='run_glue_etl_job',
        job_name=GLUE_JOB_NAME,
        region_name=REGION,
    )

    wait_for_new_file >> run_crawler >> wait_crawler_complete >> run_glue_job
