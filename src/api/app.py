from flask import Flask, render_template, request
import pandas as pd
from model_loader import load_model_from_s3

app = Flask(__name__, template_folder='templates', static_folder='static')

# Load model globally
MODEL = load_model_from_s3(
    bucket_name="mlflowartifacts12",
    key="artifacts/logistic_regression_model.pkl",
    local_path="model.pkl"
)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/predict', methods=['POST'])
def predict():
    file = request.files.get('file')
    if not file:
        return "No file uploaded.", 400

    try:
        # Read uploaded CSV
        df = pd.read_csv(file)

        # Predict
        df['Prediction'] = MODEL.predict(df)

        # Select columns to show
        display_cols = ['Prospect_ID', 'Lead_Number', 'city', 'country', 'Prediction']
        df_display = df[display_cols] if all(col in df.columns for col in display_cols) else df

        # Convert to HTML table
        result_table = df_display.to_html(classes='table', index=False)

        return render_template('result.html', table=result_table)

    except Exception as e:
        return f"Prediction failed: {str(e)}", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)
