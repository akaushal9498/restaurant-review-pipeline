"""
Process flattened Swiggy restaurant data into Parquet format.
"""

import os
import logging
import time
from constants import (
    OUTPUT_FLAT_RESTAURANTS,
    REFINED_OUTPUT_DIR,
    TEMP_CHUNKS_DIR,
    JSON_CHUNK_SIZE_MB
)
from utils import (
    create_spark_session,
    split_json_file,
    process_all_chunks,
    cleanup_chunks
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """Main function to process restaurant data."""
    # Configuration
    input_path = OUTPUT_FLAT_RESTAURANTS
    output_path = REFINED_OUTPUT_DIR
    chunks_dir = TEMP_CHUNKS_DIR

    # Check if input file exists
    if not os.path.exists(input_path):
        logger.error(f"Input file {input_path} not found")
        return

    # Get input file size
    input_size_mb = os.path.getsize(input_path) / (1024 * 1024)
    logger.info(f"Input file size: {input_size_mb:.2f} MB")

    # Adjust chunk size based on file size
    chunk_size_mb = JSON_CHUNK_SIZE_MB
    if input_size_mb > 1000:  # For files > 1GB
        chunk_size_mb = 200
    elif input_size_mb < 100:  # For smaller files
        chunk_size_mb = 20

    try:
        # Create Spark session with appropriate memory settings
        spark = create_spark_session(max_result_size="4g")

        # Split the large JSON file into smaller chunks
        chunk_files = split_json_file(input_path, chunks_dir, chunk_size_mb)

        # New columns to add
        new_columns = {
            "source": "local_processing",
            "environment": "test"
        }

        # Process all chunks and combine into a single Parquet output
        process_all_chunks(
            spark,
            chunk_files,
            output_path,
            salt_column="id",
            new_columns=new_columns
        )

        # Clean up temporary files
        cleanup_chunks(chunk_files, delete=True)

        logger.info(f"Processing complete. Output saved to {output_path}")

    except Exception as e:
        logger.error(f"Error in main process: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # Stop Spark session
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()