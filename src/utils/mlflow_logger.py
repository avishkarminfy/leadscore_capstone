# Utility to initialize MLflow experiment and logging.

import mlflow
from config.config import Config

def init_mlflow():
    mlflow.set_tracking_uri(Config.MLFLOW_TRACKING_URI)
    mlflow.set_experiment(Config.EXPERIMENT_NAME)


def log_model_to_registry(model, model_name, registered_model_name):
    mlflow.sklearn.log_model(model, artifact_path=model_name, registered_model_name=registered_model_name)
