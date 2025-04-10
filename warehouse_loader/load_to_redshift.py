import pandas as pd
import sqlalchemy
from utils.config import REDSHIFT_CONN_STRING
from utils.logger import get_logger
import os

logger = get_logger("RedshiftLoader")

def load_parquet_to_redshift(parquet_path, table_name):
    logger.info(f"Loading data from {parquet_path} to Redshift table {table_name}")
    engine = sqlalchemy.create_engine(REDSHIFT_CONN_STRING)
    conn = engine.connect()

    # Read the Parquet file into a Pandas DataFrame
    df = pd.read_parquet(parquet_path)

    try:
        df.to_sql(table_name, conn, if_exists="append", index=False, method="multi", chunksize=1000)
        logger.info(f"Successfully loaded data to {table_name}")
    except Exception as e:
        logger.error(f"Error loading data to Redshift: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    load_parquet_to_redshift("data/processed/swiggy_restaurants.parquet", "swiggy_restaurants")
    load_parquet_to_redshift("data/processed/streaming_reviews.parquet", "amazon_reviews")
