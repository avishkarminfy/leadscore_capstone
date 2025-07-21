# Compare batch or streaming data to baseline using Evidently.

from evidently.report import Report
from evidently.metric_preset import DataDriftPreset
import pandas as pd

def run_drift_report(reference_df: pd.DataFrame, new_df: pd.DataFrame, output_path: str):
    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=reference_df, current_data=new_df)
    report.save_html(output_path)
    print(f"Drift report saved at {output_path}")
