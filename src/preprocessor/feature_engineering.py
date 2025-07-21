import numpy as np

def group_lead_source(df):
    df['lead_source'] = df['lead_source'].replace({
        'google': 'Google',
        'Pay per Click Ads': 'Google',
        'Live Chat': 'Olark Chat',
        'bing': 'Organic Search'
    })

    df['lead_source'] = df['lead_source'].apply(
        lambda x: 'Referral Sites' if 'blog' in str(x).lower() else x
    )

    df['lead_source'] = df['lead_source'].apply(
        lambda x: x if x in df['lead_source'].value_counts().head(8) else 'Other'
    )
    return df

def group_last_activity_columns(df):
    for col in ['last_activity', 'last_notable_activity']:
        df[col] = df[col].replace({
            'Email Received': 'SMS/Email Sent',
            'SMS Sent': 'SMS/Email Sent',
            'Email Marked Spam': 'Not interested in email',
            'Email Bounced': 'Not interested in email',
            'Unsubscribed': 'Not interested in email',
            'Resubscribed to emails': 'Email Opened',
            'Visited Booth in Tradeshow': 'Page Visited on Website',
            'View in browser link Clicked': 'Page Visited on Website'
        })
    return df

def clean_country(df):
    df['country'] = df['country'].apply(
        lambda x: np.nan if x in ['Unknown', 'Asia/Pacific Region'] else x
    )
    return df

def standardize_specialization(df):
    df['specialization'] = df['specialization'].replace({
        'E-COMMERCE': 'E-commerce',
        'E-Business': 'E-commerce',
        'Banking, Investment And Insurance': 'Finance Management',
        'Media and Advertising': 'Marketing Management',
        'Select': 'Not Provided'
    })
    return df

def clean_hear_about_x_education(df):
    df['how_did_you_hear_about_x_education'] = df['how_did_you_hear_about_x_education'].replace({
        'Select': 'Not Provided',
        'SMS': 'SMS/Email',
        'Email': 'SMS/Email'
    })
    return df

def simplify_what_matters_most(df):
    df['what_matters_most_to_you_in_choosing_a_course'] = df[
        'what_matters_most_to_you_in_choosing_a_course'
    ].replace({
        'Flexibility & Convenience': 'Better Career Prospects',
        'Other': 'Better Career Prospects'
    })
    return df

def clean_lead_profile_and_city(df):
    df['lead_profile'] = df['lead_profile'].replace({'Select': 'Not Assigned'})
    df['city'] = df['city'].replace({'Select': 'Not Provided'})
    return df
