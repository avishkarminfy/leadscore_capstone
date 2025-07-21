from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from detect_drift import run_drift_report

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 15),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_drift_detection_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["drift", "monitoring", "ml"],
    description="Checks for data drift using EvidentlyAI and logs result to S3"
) as dag:

    start = DummyOperator(task_id="start")

    fetch_data_from_redshift = DummyOperator(task_id="fetch_data_from_redshift")

    preprocess_data = DummyOperator(task_id="preprocess_input_data")

    check_data_drift = PythonOperator(
        task_id="check_data_drift",
        python_callable=run_drift_report,
        op_kwargs={
            "execution_date": "{{ ds }}",  # Templated
        },
    )

    send_drift_flag = DummyOperator(task_id="send_drift_flag_notification")

    end = DummyOperator(task_id="end")

    # Set Task Flow
    start >> fetch_data_from_redshift >> preprocess_data >> check_data_drift >> send_drift_flag >> end
