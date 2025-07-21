import pandas as pd
import joblib
import json
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score

def main():
    print("Loading preprocessed data from /tmp/...")
    X = pd.read_csv("/tmp/X.csv")
    y = pd.read_csv("/tmp/y.csv").squeeze()

    print("Splitting data for training and testing...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, stratify=y, test_size=0.2, random_state=42
    )

    categorical_cols = X.select_dtypes(include='object').columns.tolist()
    numerical_cols = X.select_dtypes(include=['int64', 'float64']).columns.tolist()

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

    models = {
        "logistic_regression": LogisticRegression(max_iter=1000, random_state=42),
        "random_forest": RandomForestClassifier(n_estimators=100, random_state=42),
        "xgboost": XGBClassifier(use_label_encoder=False, eval_metric='logloss', random_state=42)
    }

    best_auc = 0
    best_pipeline = None
    best_model_name = ""
    results = {}

    print("Starting model training loop...")
    for name, model in models.items():
        print(f"--- Training {name} ---")
        pipeline = Pipeline([
            ('preprocessor', preprocessor),
            ('model', model)
        ])
        pipeline.fit(X_train, y_train)

        y_pred = pipeline.predict(X_test)
        y_proba = pipeline.predict_proba(X_test)[:, 1]

        acc = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)
        auc = roc_auc_score(y_test, y_proba)
        
        print(f"Results for {name}: Accuracy={acc:.4f}, F1={f1:.4f}, AUC={auc:.4f}")
        results[name] = {'accuracy': acc, 'f1_score': f1, 'roc_auc': auc}

        if auc > best_auc:
            best_auc = auc
            best_pipeline = pipeline
            best_model_name = name

    print(f"Best model found: {best_model_name} with AUC: {best_auc:.4f}")
    if best_pipeline:
        output_path = f"/tmp/best_model_pipeline.pkl"
        joblib.dump(best_pipeline, output_path)
        print(f"Best model pipeline saved to {output_path}")

        # You can optionally upload this file to S3 for persistence
        # s3 = boto3.client('s3')
        # s3.upload_file(output_path, 'your-mwaa-bucket', f'models/best_model_pipeline.pkl')
        
if __name__ == "__main__":
    main()