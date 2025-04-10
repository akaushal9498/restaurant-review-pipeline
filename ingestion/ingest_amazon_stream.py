import sqlite3
import pandas as pd
import time
from utils.logger import get_logger
from utils.config import SQLITE_DB_PATH, STREAM_OUTPUT_PATH

logger = get_logger("IngestAmazonStream")

BATCH_SIZE = 100
SLEEP_TIME = 10  # seconds


def fetch_latest_reviews(cursor, last_id):
    query = f"""
        SELECT * FROM Reviews
        WHERE Id > {last_id}
        ORDER BY Id ASC
        LIMIT {BATCH_SIZE};
    """
    return pd.read_sql_query(query, con=conn)


def append_to_stream_file(df):
    df.to_csv(STREAM_OUTPUT_PATH, mode='a', index=False, header=not os.path.exists(STREAM_OUTPUT_PATH))


if __name__ == '__main__':
    import os

    logger.info("Starting streaming ingestion from SQLite")
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()

    last_id = 0

    while True:
        try:
            new_data = fetch_latest_reviews(cursor, last_id)
            if not new_data.empty:
                append_to_stream_file(new_data)
                last_id = new_data['Id'].max()
                logger.info(f"Fetched {len(new_data)} new rows. Last ID: {last_id}")
            else:
                logger.info("No new data. Sleeping...")
            time.sleep(SLEEP_TIME)
        except Exception as e:
            logger.error(f"Error during streaming ingestion: {e}")
            break

    conn.close()