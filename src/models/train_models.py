import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier

from preprocessor.pipeline import run_pipeline  
from utils.mlflow_logger import setup_mlflow_experiment
from utils.logger import get_logger

# Initialize logger
logger = get_logger(__name__)

# Step 1: Load data from Amazon Redshift
def load_data_from_redshift():
    try:
        redshift_url = "postgresql://Admin:Am7841043635@redshift-cluster-1.region.redshift.amazonaws.com:5439/dev"
        query = "SELECT * FROM leads_dataset;"
        engine = create_engine(redshift_url)
        df = pd.read_sql(query, engine)
        logger.info("Data successfully loaded from Redshift.")
        return df
    except Exception as e:
        logger.error(f"Error loading data from Redshift: {str(e)}")
        raise

# Step 2: Train models
def train_models(df: pd.DataFrame):
    # Run your cleaning + feature engineering + encoding pipeline
    df_processed = run_pipeline(df)

    # Separate features and target
    X = df_processed.drop(columns=["converted"])
    y = df_processed["converted"]

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Define models
    models = {
        "Logistic Regression": LogisticRegression(max_iter=1000),
        "Random Forest": RandomForestClassifier(n_estimators=100),
        "XGBoost": XGBClassifier(use_label_encoder=False, eval_metric='logloss')
    }

    best_model = None
    best_auc = 0

    # MLflow tracking setup
    mlflow.set_tracking_uri("https://183248601668.mlflow.sagemaker.ap-south-1.amazonaws.com")  
    mlflow.set_experiment("LeadConversion-Automated")

    for name, model in models.items():
        with mlflow.start_run(run_name=name):
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)
            y_proba = model.predict_proba(X_test)[:, 1]

            acc = accuracy_score(y_test, y_pred)
            auc = roc_auc_score(y_test, y_proba)

            mlflow.log_param("model_name", name)
            mlflow.log_metric("accuracy", acc)
            mlflow.log_metric("auc", auc)
            mlflow.sklearn.log_model(model, artifact_path="model")

            logger.info(f"{name} - Accuracy: {acc:.4f}, AUC: {auc:.4f}")

            if auc > best_auc:
                best_auc = auc
                best_model = model

    logger.info(f"Best model selected with AUC: {best_auc:.4f}")

if __name__ == "__main__":
    df = load_data_from_redshift()
    train_models(df)
