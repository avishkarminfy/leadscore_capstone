# Lead Conversion Prediction Pipeline (AWS + MLflow + Airflow)

A fully automated, modular, and production-ready Machine Learning pipeline to **predict B2B lead conversion likelihood** as High, Medium, or Low using behavioral and demographic data.  
This solution is built with **Amazon Web Services (AWS)**, **MLflow**, **Evidently AI**, and **Apache Airflow (MWAA)** for end-to-end orchestration.

---

## 📦 Project Structure

```text
lead-conversion-prediction/
├── src/
│   ├── data_processing/
│   │   ├── data_cleaning.py
│   │   └── feature_engineering.py
│   ├── models/
│   │   ├── train_model.py
│   │   └── evaluate_model.py
│   ├── monitoring/
│   │   ├── detect_drift.py
│   │   └── generate_shap.py
│   ├── utils/
│   │   └── s3_utils.py
│   ├── api/
│   │   ├── app.py
│   │   └── templates/
│   │       ├── index.html
│   │       └── result.html
├── airflow_dags/
│   ├── data_ingestion_dag.py
│   ├── data_drift_detection_dag.py
│   └── retraining_dag.py
├── artifacts/
├── requirements.txt
└── README.md

---
```
## ML Models Used

- Logistic Regression
- Random Forest
- XGBoost (selected based on AUC)

> Best model is registered to MLflow and pushed to S3.
<img width="940" height="502" alt="image" src="https://github.com/user-attachments/assets/62554618-9955-4492-9e9b-f881ca1d3c01" />

---

## 🧪 MLflow Tracking & Explainability

- MLflow logs: metrics, parameters, artifacts
- SHAP Explainability plots
- HTML drift reports using **Evidently AI**
<img width="940" height="505" alt="image" src="https://github.com/user-attachments/assets/ecda4070-243f-47c2-8f9a-079ebaaec886" />

📸 **MLflow UI Screenshot**  
<img width="940" height="508" alt="image" src="https://github.com/user-attachments/assets/3d397b96-9d1b-442b-90db-3a512076b83c" />

📸 **Evidently Drift Report Screenshot**  
<img width="1919" height="1034" alt="image" src="https://github.com/user-attachments/assets/350249a0-1885-4ea8-a41c-490d1daf6bdb" />

---

## 🔁 MWAA DAGs

| DAG Name | Description |
|----------|-------------|
| `data_ingestion_dag.py` | Triggers AWS Glue job to load data from S3 → Redshift |
| `data_drift_detection_dag.py` | Compares train vs new data using Evidently |
| `retraining_dag.py` | Monthly retraining and best model update |

📸 **Airflow DAG UI Screenshot**
### data_ingestion_dag.py
<img width="940" height="504" alt="image" src="https://github.com/user-attachments/assets/59bc1a94-848a-4aae-bf1d-1e8880bf12bb" />

### data_drift_detection_dag.py
<img width="1919" height="1035" alt="image (1)" src="https://github.com/user-attachments/assets/2d867eff-bc77-41a2-8d0a-fe0f966b867e" />

### `retraining_dag.py`
<img width="1919" height="1033" alt="image (2)" src="https://github.com/user-attachments/assets/57538cf7-1502-4a4a-8499-43920a8e4180" />

---

## 🖥️ Flask Batch Prediction UI

- Upload CSV file with multiple leads
- Get prediction for each row in styled HTML table
- Works locally or can be exposed via ngrok

<img width="940" height="507" alt="image" src="https://github.com/user-attachments/assets/2e601cee-092a-4f65-981f-784262127dd2" />
<img width="940" height="504" alt="image" src="https://github.com/user-attachments/assets/a89ef775-3e76-43e9-b02d-30718c76ccae" />
---

## 💻 How to Run Locally

```bash
# 1. Clone Repository
git clone https://github.com/yourusername/lead-conversion-prediction.git
cd lead-conversion-prediction

# 2. Set Up Environment
pip install -r requirements.txt

# 3. Export AWS Credentials (if needed)
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=ap-south-1

# 4. Train the Model (Locally)
python src/models/train_model.py

# 5. Generate Drift Report (Locally)
python src/monitoring/detect_drift.py

# 6. Run Flask UI for Batch Prediction
cd src/api
python app.py
# Then open your browser at:
# http://127.0.0.1:5000
```

---

## ✅ Output Examples

- **AUC**: `0.91`
- **Best Model**: `XGBoost`
- **Drift Detected**: `False`
- **Model Registered to MLflow**: ✔️

---





