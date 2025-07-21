# data_ingestion/data_loader.py

import pandas as pd
import psycopg2
from config.config import *

def load_data_from_redshift():
    try:
        conn = psycopg2.connect(
            host=REDSHIFT_HOST,
            dbname=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD,
            port=REDSHIFT_PORT
        )
        query = f"SELECT * FROM {REDSHIFT_TABLE}"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        raise RuntimeError(f"Failed to fetch data from Redshift: {e}")
