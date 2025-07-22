from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import boto3
import pandas as pd
import joblib
import tempfile
import os
import json
from io import BytesIO

# ===== CONFIGURATION - UPDATE THESE VALUES =====
S3_BUCKET = 'leadscoring-pipeline-data'         # Your S3 bucket
PROCESSED_DATA_PREFIX = 'processed/'             # Where processed data is stored
MODELS_PREFIX = 'models/'                        # Where to save trained models
REGION = 'ap-south-1'                           # Your AWS region
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

def download_latest_data(**context):
    """Download the latest processed data from S3"""
    s3 = boto3.client('s3', region_name=REGION)
    
    # Get execution date
    execution_date = context['ds']
    
    # List files in processed folder
    prefix = f"{PROCESSED_DATA_PREFIX}{execution_date}/"
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    
    if 'Contents' not in response:
        # If no files for today, get the latest file
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=PROCESSED_DATA_PREFIX)
        if 'Contents' not in response:
            raise ValueError(f"No processed data found in s3://{S3_BUCKET}/{PROCESSED_DATA_PREFIX}")
    
    # Get the most recent CSV file
    csv_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.csv')]
    latest_file = sorted(csv_files, key=lambda x: x['LastModified'], reverse=True)[0]
    
    # Download file
    print(f"Downloading data from s3://{S3_BUCKET}/{latest_file['Key']}")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=latest_file['Key'])
    df = pd.read_csv(obj['Body'])
    
    # Save to temp location
    temp_path = '/tmp/training_data.csv'
    df.to_csv(temp_path, index=False)
    
    print(f"Downloaded {len(df)} rows of training data")
    
    # Push file path to XCom
    context['task_instance'].xcom_push(key='data_path', value=temp_path)
    
    return temp_path

def preprocess_data(**context):
    """Run preprocessing on the data"""
    import subprocess
    import sys
    
    # Get data path from previous task
    data_path = context['task_instance'].xcom_pull(task_ids='download_data', key='data_path')
    
    print(f"Running preprocessing on {data_path}")
    
    # Run preprocessing script
    cmd = [
        sys.executable, 
        '/usr/local/airflow/dags/scripts/preprocess.py',
        '--s3_bucket', S3_BUCKET,
        '--s3_key', data_path.replace(f's3://{S3_BUCKET}/', '')
    ]
    
    # Since we're working with local file, modify preprocessing to work locally
    # For now, let's do inline preprocessing
    df = pd.read_csv(data_path)
    
    # Basic preprocessing (simplified version of your preprocess.py)
    # Drop unique identifiers
    df = df.drop(['Prospect ID', 'Lead Number'], axis=1, errors='ignore')
    
    # Convert binary columns
    binary_cols = ['Do Not Email', 'Do Not Call', 'Search', 'Magazine', 
                   'Newspaper Article', 'X Education Forums', 'Newspaper', 
                   'Digital Advertisement', 'Through Recommendations']
    
    for col in binary_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: 1 if x == 'Yes' else 0)
    
    # Standardize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    
    # Drop rows with missing target
    df = df.dropna(subset=['converted'])
    
    # Split features and target
    X = df.drop(columns=['converted'])
    y = df['converted']
    
    # Save preprocessed data
    X.to_csv('/tmp/X.csv', index=False)
    y.to_csv('/tmp/y.csv', index=False)
    
    print(f"Preprocessing complete. X shape: {X.shape}, y shape: {y.shape}")
    
    return True

def train_model(**context):
    """Train the model using simplified training logic"""
    from sklearn.model_selection import train_test_split
    from sklearn.pipeline import Pipeline
    from sklearn.compose import ColumnTransformer
    from sklearn.impute import SimpleImputer
    from sklearn.preprocessing import OneHotEncoder
    from sklearn.linear_model import LogisticRegression
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
    
    print("Loading preprocessed data...")
    X = pd.read_csv("/tmp/X.csv")
    y = pd.read_csv("/tmp/y.csv").squeeze()
    
    print("Splitting data...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, stratify=y, test_size=0.2, random_state=42
    )
    
    # Identify column types
    categorical_cols = X.select_dtypes(include='object').columns.tolist()
    numerical_cols = X.select_dtypes(include=['int64', 'float64']).columns.tolist()
    
    # Create preprocessor
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', SimpleImputer(strategy='mean'), numerical_cols),
            ('cat', Pipeline([
                ('imputer', SimpleImputer(strategy='most_frequent')),
                ('encoder', OneHotEncoder(handle_unknown='ignore'))
            ]), categorical_cols)
        ],
        remainder='passthrough'
    )
    
    # For simplicity, let's just use LogisticRegression
    print("Training Logistic Regression model...")
    
    pipeline = Pipeline([
        ('preprocessor', preprocessor),
        ('model', LogisticRegression(max_iter=1000, random_state=42))
    ])
    
    pipeline.fit(X_train, y_train)
    
    # Evaluate
    y_pred = pipeline.predict(X_test)
    y_proba = pipeline.predict_proba(X_test)[:, 1]
    
    accuracy = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_proba)
    
    print(f"Model Performance: Accuracy={accuracy:.4f}, F1={f1:.4f}, AUC={auc:.4f}")
    
    # Save model to temp location
    model_path = '/tmp/model_pipeline.pkl'
    joblib.dump(pipeline, model_path)
    
    print(f"Model saved to {model_path}")
    
    # Push metrics to XCom
    context['task_instance'].xcom_push(key='model_path', value=model_path)
    context['task_instance'].xcom_push(key='metrics', value={
        'accuracy': accuracy,
        'f1_score': f1,
        'auc': auc
    })
    
    return model_path

def upload_model_to_s3(**context):
    """Upload trained model to S3"""
    s3 = boto3.client('s3', region_name=REGION)
    
    # Get model path and metrics
    model_path = context['task_instance'].xcom_pull(task_ids='train_model', key='model_path')
    metrics = context['task_instance'].xcom_pull(task_ids='train_model', key='metrics')
    execution_date = context['ds']
    
    # Create model key with timestamp
    model_key = f"{MODELS_PREFIX}model_{execution_date}.pkl"
    
    # Upload model
    print(f"Uploading model to s3://{S3_BUCKET}/{model_key}")
    s3.upload_file(model_path, S3_BUCKET, model_key)
    
    # Also save as latest model
    latest_key = f"{MODELS_PREFIX}latest_model.pkl"
    s3.copy_object(
        Bucket=S3_BUCKET,
        CopySource={'Bucket': S3_BUCKET, 'Key': model_key},
        Key=latest_key
    )
    
    # Save model metadata
    metadata = {
        'model_path': f"s3://{S3_BUCKET}/{model_key}",
        'training_date': execution_date,
        'metrics': metrics,
        'triggered_by': context.get('dag_run', {}).conf.get('triggered_by', 'manual')
    }
    
    metadata_key = f"{MODELS_PREFIX}model_{execution_date}_metadata.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=metadata_key,
        Body=json.dumps(metadata, indent=2)
    )
    
    print(f"Model and metadata uploaded successfully")
    print(f"Model metrics: {metrics}")
    
    # Clean up temp files
    os.remove(model_path)
    os.remove('/tmp/X.csv')
    os.remove('/tmp/y.csv')
    os.remove('/tmp/training_data.csv')
    
    return f"s3://{S3_BUCKET}/{model_key}"

with DAG(
    dag_id='retrain_model_simple_dag',
    default_args=default_args,
    description='Simple model retraining without SageMaker',
    schedule_interval=None,  # Triggered by drift detection
    catchup=False,
    tags=['retraining', 'ml-pipeline', 'simple'],
) as dag:

    # Task 1: Download latest data
    download_data = PythonOperator(
        task_id='download_data',
        python_callable=download_latest_data,
        provide_context=True,
    )

    # Task 2: Preprocess data
    preprocess = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data,
        provide_context=True,
    )

    # Task 3: Train model
    train = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
        provide_context=True,
    )

    # Task 4: Upload model to S3
    upload_model = PythonOperator(
        task_id='upload_model',
        python_callable=upload_model_to_s3,
        provide_context=True,
    )

    # Define workflow
    download_data >> preprocess >> train >> upload_model