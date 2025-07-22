# Simple preprocessing script for MWAA
# This is a simplified version that works locally in Airflow

import pandas as pd
import numpy as np
import argparse

def preprocess_data(input_path, output_x_path, output_y_path):
    """Simple preprocessing for lead scoring data"""
    
    # Read data
    df = pd.read_csv(input_path)
    
    # Drop unique identifiers
    df = df.drop(['Prospect ID', 'Lead Number'], axis=1, errors='ignore')
    
    # Convert binary columns
    binary_cols = [
        'Do Not Email', 'Do Not Call', 'Search', 'Magazine', 
        'Newspaper Article', 'X Education Forums', 'Newspaper', 
        'Digital Advertisement', 'Through Recommendations',
        'Receive More Updates About Our Courses',
        'Update me on Supply Chain Content',
        'Get updates on DM Content',
        'I agree to pay the amount through cheque',
        'A free copy of Mastering The Interview'
    ]
    
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
    
    # Save
    X.to_csv(output_x_path, index=False)
    y.to_csv(output_y_path, index=False)
    
    print(f"Preprocessing complete. X shape: {X.shape}, y shape: {y.shape}")
    
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output_x", default="/tmp/X.csv")
    parser.add_argument("--output_y", default="/tmp/y.csv")
    args = parser.parse_args()
    
    preprocess_data(args.input, args.output_x, args.output_y)