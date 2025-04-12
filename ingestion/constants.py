"""Constants used throughout the data processing pipeline."""

# File paths
INPUT_FILE = "/Users/anupkaushal/PycharmProjects/restaurant-review-pipeline/data/data.json"
OUTPUT_FLAT_RESTAURANTS = "data/flat_restaurants.json"
OUTPUT_FLAT_MENU = "data/flat_menu.json"
SQLITE_DB_PATH = "/Users/anupkaushal/PycharmProjects/restaurant-review-pipeline/data/database.sqlite"
JDBC_DRIVER_PATH = "/Users/anupkaushal/PycharmProjects/restaurant-review-pipeline/jar_files/sqlite-jdbc-3.49.1.0.jar"

# Output directories
PARQUET_OUTPUT_DIR = "parquet_output/"
PARQUET_STREAM_DIR = "parquet_stream/"
CHECKPOINT_DIR = "checkpoint/stream_checkpoint"
REFINED_OUTPUT_DIR = "data/refined/processed"
TEMP_CHUNKS_DIR = "data/temp_chunks"

# Database config
JDBC_URL = f"jdbc:sqlite:{SQLITE_DB_PATH}"
REVIEWS_TABLE = "Reviews"

# Processing configs
STREAM_CHUNK_SIZE = 2000  # Records per chunk for Amazon streaming
STREAM_DELAY_SECONDS = 5   # Delay between stream chunks
JSON_CHUNK_SIZE_MB = 100   # Default size for JSON chunks in MB
SALT_KEY = "salt_key"      # Salt key for hashing

# Spark configs
DEFAULT_APP_NAME = "DataProcessingPipeline"
SPARK_CONFIGS = {
    "spark.driver.memory": "4g",
    "spark.executor.memory": "4g",
    "spark.driver.maxResultSize": "4g",
    "spark.sql.shuffle.partitions": "20",
    "spark.default.parallelism": "20",
    "spark.sql.files.ignoreCorruptFiles": "true"
}

# Schema definitions
REVIEW_SCHEMA_FIELDS = [
    {"name": "Id", "type": "integer", "nullable": True},
    {"name": "ProductId", "type": "string", "nullable": True},
    {"name": "UserId", "type": "string", "nullable": True},
    {"name": "ProfileName", "type": "string", "nullable": True},
    {"name": "HelpfulnessNumerator", "type": "integer", "nullable": True},
    {"name": "HelpfulnessDenominator", "type": "integer", "nullable": True},
    {"name": "Score", "type": "integer", "nullable": True},
    {"name": "Time", "type": "integer", "nullable": True},
    {"name": "Summary", "type": "string", "nullable": True},
    {"name": "Text", "type": "string", "nullable": True},
    {"name": "ingest_time", "type": "timestamp", "nullable": True}
]

# Validation
REQUIRED_RESTAURANT_COLUMNS = ["id", "name", "city"]