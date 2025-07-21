import pandas as pd
import numpy as np
import boto3
import argparse

def data_cleaning(df):
    # Drop unique identifiers that do not contribute to model learning
    df = df.drop(['Prospect ID', 'Lead Number'], axis=1, errors='ignore')

    # Standardize values in 'Asymmetrique Activity Index' and 'Asymmetrique Profile Index'
    for col in ['Asymmetrique Activity Index', 'Asymmetrique Profile Index']:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.split('.', expand=True)[0]
                .replace({'01': '3', '02': '2', '03': '1'})
                .astype(float)
            )

    # Convert binary categorical columns from 'Yes'/'No' to 1/0
    binary_cols = [
        'Do Not Email', 'Do Not Call', 'Search', 'Magazine', 'Newspaper Article',
        'X Education Forums', 'Newspaper', 'Digital Advertisement', 'Through Recommendations',
        'Receive More Updates About Our Courses', 'Update me on Supply Chain Content',
        'Get updates on DM Content', 'I agree to pay the amount through cheque',
        'A free copy of Mastering The Interview'
    ]
    for col in binary_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: 1 if x == 'Yes' else 0)

    # Standardize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_')
    )
    return df

def initial_feature_engineering(df):
    # Standardizing Lead Source
    if 'lead_source' in df.columns:
        df['lead_source'] = df['lead_source'].replace({
            'google': 'Google', 'Pay per Click Ads': 'Google',
            'Live Chat': 'Olark Chat', 'bing': 'Organic Search'
        })
        df['lead_source'] = df['lead_source'].apply(
            lambda x: 'Referral Sites' if 'blog' in str(x).lower() else x
        )
        top_sources = df['lead_source'].value_counts().nlargest(8).index
        df['lead_source'] = df['lead_source'].apply(
            lambda x: x if x in top_sources else 'Other'
        )

    # Grouping Activity columns
    for col in ['last_activity', 'last_notable_activity']:
        if col in df.columns:
            df[col] = df[col].replace({
                'Email Received': 'SMS/Email Sent', 'SMS Sent': 'SMS/Email Sent',
                'Email Marked Spam': 'Not interested in email', 'Email Bounced': 'Not interested in email',
                'Unsubscribed': 'Not interested in email', 'Resubscribed to emails': 'Email Opened',
                'Visited Booth in Tradeshow': 'Page Visited on Website',
                'View in browser link Clicked': 'Page Visited on Website'
            })
    # Clean other categorical columns
    if 'country' in df.columns:
        df['country'] = df['country'].apply(lambda x: np.nan if x in ['Unknown', 'Asia/Pacific Region'] else x)
    if 'specialization' in df.columns:
        df['specialization'] = df['specialization'].replace({
            'E-COMMERCE': 'E-commerce', 'E-Business': 'E-commerce',
            'Banking, Investment And Insurance': 'Finance Management',
            'Media and Advertising': 'Marketing Management', 'Select': 'Not Provided'
        })
    if 'how_did_you_hear_about_x_education' in df.columns:
        df['how_did_you_hear_about_x_education'] = df['how_did_you_hear_about_x_education'].replace({'Select': 'Not Provided', 'SMS': 'SMS/Email', 'Email': 'SMS/Email'})
    if 'what_matters_most_to_you_in_choosing_a_course' in df.columns:
        df['what_matters_most_to_you_in_choosing_a_course'] = df['what_matters_most_to_you_in_choosing_a_course'].replace({'Flexibility & Convenience': 'Better Career Prospects', 'Other': 'Better Career Prospects'})
    if 'lead_profile' in df.columns:
        df['lead_profile'] = df['lead_profile'].replace({'Select': 'Not Assigned'})
    if 'city' in df.columns:
        df['city'] = df['city'].replace({'Select': 'Not Provided'})
    
    return df

def main(s3_bucket, s3_key):
    print(f"Loading data from s3://{s3_bucket}/{s3_key}")
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    df = pd.read_csv(obj['Body'])

    print("Cleaning data...")
    df = data_cleaning(df)
    
    print("Performing feature engineering...")
    df = initial_feature_engineering(df)
    
    print("Dropping rows with missing target variable...")
    df = df.dropna(subset=['converted'])

    X = df.drop(columns=['converted'])
    y = df['converted']

    print(f"Saving preprocessed data to /tmp/... X shape: {X.shape}, y shape: {y.shape}")
    X.to_csv("/tmp/X.csv", index=False)
    y.to_csv("/tmp/y.csv", index=False)
    
    print("Preprocessing complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3_bucket", required=True, help="S3 bucket for the raw data file")
    parser.add_argument("--s3_key", required=True, help="S3 key for the raw data file")
    args = parser.parse_args()
    main(args.s3_bucket, args.s3_key)