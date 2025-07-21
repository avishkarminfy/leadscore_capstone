import pandas as pd

def drop_identifier_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop identifier columns not useful for ML."""
    return df.drop(['Prospect ID', 'Lead Number'], axis=1)
