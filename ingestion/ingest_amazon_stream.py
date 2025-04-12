"""
Creates a mock streaming scenario by reading data from SQLite in chunks.
"""

import time
import logging
from constants import (
    JDBC_URL, REVIEWS_TABLE, STREAM_CHUNK_SIZE,
    STREAM_DELAY_SECONDS, PARQUET_OUTPUT_DIR
)
from utils import create_spark_session
from pyspark.sql.functions import current_timestamp

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """Main function to simulate streaming from SQLite database."""
    try:
        # Initialize SparkSession
        spark = create_spark_session(app_name="StreamSimulator")

        # Simulate streaming with chunks
        chunk_size = STREAM_CHUNK_SIZE
        offset = 0
        chunk_id = 1

        while True:
            logger.info(f"Reading chunk {chunk_id}...")

            # Create JDBC query using subquery with LIMIT + OFFSET
            query = f"(SELECT * FROM {REVIEWS_TABLE} LIMIT {chunk_size} OFFSET {offset}) AS chunk"

            # Read chunk into Spark DataFrame
            df = spark.read.format("jdbc") \
                .option("url", JDBC_URL) \
                .option("dbtable", query) \
                .option("driver", "org.sqlite.JDBC") \
                .load()

            # Break if no more data
            if df.rdd.isEmpty():
                logger.info("No more data to read.")
                break

            # Add current timestamp
            df = df.withColumn("ingest_time", current_timestamp())

            # Save as Parquet
            df.write.mode("append").parquet(PARQUET_OUTPUT_DIR)

            logger.info(f"Chunk {chunk_id} written.")

            # Sleep to simulate streaming delay
            time.sleep(STREAM_DELAY_SECONDS)

            # Update for next chunk
            offset += chunk_size
            chunk_id += 1

    except Exception as e:
        logger.error(f"Error in stream simulation: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # Stop Spark session
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()