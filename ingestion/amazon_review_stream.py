"""
Process streaming Parquet files and write to a streaming output location.
"""

import logging
from constants import PARQUET_OUTPUT_DIR, PARQUET_STREAM_DIR, CHECKPOINT_DIR
from utils import create_spark_session, get_review_schema
from pyspark.sql.functions import current_timestamp

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """Main function to process the streaming data."""
    try:
        # Init Spark
        spark = create_spark_session(app_name="StreamProcessor")
        spark.sparkContext.setLogLevel("WARN")

        # Get schema for the review data
        schema = get_review_schema()

        # Streaming read from parquet_output with the defined schema
        df_stream = spark.readStream.schema(schema).parquet(PARQUET_OUTPUT_DIR)

        # Add current timestamp
        df_enriched = df_stream.withColumn("read_timestamp", current_timestamp())

        # Write stream to parquet_stream
        query = df_enriched.writeStream \
            .format("parquet") \
            .outputMode("append") \
            .option("checkpointLocation", CHECKPOINT_DIR) \
            .option("path", PARQUET_STREAM_DIR) \
            .start()

        logger.info("Stream processing started. Awaiting termination...")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in stream processing: {e}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()