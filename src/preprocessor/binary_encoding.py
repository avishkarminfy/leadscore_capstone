def encode_binary_columns(df):
    """
    Convert 'Yes'/'No' binary columns to 1/0.
    """
    binary_cols = [
        'Do Not Email', 'Do Not Call', 'Search', 'Magazine', 'Newspaper Article',
        'X Education Forums', 'Newspaper', 'Digital Advertisement', 'Through Recommendations',
        'Receive More Updates About Our Courses', 'Update me on Supply Chain Content',
        'Get updates on DM Content', 'I agree to pay the amount through cheque',
        'A free copy of Mastering The Interview'
    ]
    df[binary_cols] = df[binary_cols].applymap(lambda x: 1 if x == 'Yes' else 0)
    return df
