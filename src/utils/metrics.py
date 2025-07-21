from sklearn.metrics import accuracy_score, roc_auc_score, confusion_matrix, classification_report
import mlflow

def evaluate_model(name, model, X_test, y_test, log_to_mlflow=False):
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    acc = accuracy_score(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_prob)

    print(f"\n📊 {name} Evaluation")
    print("Accuracy      :", acc)
    print("ROC AUC Score :", roc_auc)
    print("Confusion Matrix:\n", confusion_matrix(y_test, y_pred))
    print("Classification Report:\n", classification_report(y_test, y_pred))

    if log_to_mlflow:
        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("roc_auc", roc_auc)
