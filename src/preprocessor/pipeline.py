import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder

# Custom preprocessing steps
from preprocessor.cleaning import (
    drop_identifier_columns,
    standardize_column_names,
    standardize_asymmetrique_indexes
)
from preprocessor.encoding import encode_binary_columns
from preprocessor.feature_engineering import (
    group_lead_source,
    group_last_activity_columns,
    clean_country,
    standardize_specialization,
    clean_hear_about_x_education,
    simplify_what_matters_most,
    clean_lead_profile_and_city
)

def custom_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """Run all custom cleaning and feature engineering steps"""
    df = drop_identifier_columns(df)
    df = standardize_asymmetrique_indexes(df)
    df = encode_binary_columns(df)
    df = standardize_column_names(df)
    df = group_lead_source(df)
    df = group_last_activity_columns(df)
    df = clean_country(df)
    df = standardize_specialization(df)
    df = clean_hear_about_x_education(df)
    df = simplify_what_matters_most(df)
    df = clean_lead_profile_and_city(df)
    return df

def build_preprocessing_pipeline(X: pd.DataFrame):
    """Build a ColumnTransformer pipeline for numeric and categorical processing"""

    categorical_cols = X.select_dtypes(include='object').columns.tolist()
    numerical_cols = X.select_dtypes(include=['int64', 'float64']).columns.tolist()

    preprocessor = ColumnTransformer([
        ('num', SimpleImputer(strategy='mean'), numerical_cols),
        ('cat', Pipeline([
            ('imputer', SimpleImputer(strategy='most_frequent')),
            ('encoder', OneHotEncoder(handle_unknown='ignore'))
        ]), categorical_cols)
    ])

    return preprocessor
