"""Utility functions for data processing pipeline."""

import os
import json
import time
import logging
import subprocess
import shutil
import psutil
from tqdm import tqdm
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha2, concat, current_timestamp, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from constants import (
    SPARK_CONFIGS, JDBC_DRIVER_PATH, SALT_KEY, REQUIRED_RESTAURANT_COLUMNS,
    REVIEW_SCHEMA_FIELDS
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_spark_session(app_name="DataProcessingPipeline", max_result_size="4g"):
    """
    Create and return a Spark session with optimized settings.

    Args:
        app_name: Name of the Spark application
        max_result_size: Maximum result size for Spark driver

    Returns:
        SparkSession: Configured Spark session
    """
    builder = SparkSession.builder.appName(app_name).master("local[*]")

    # Add all configurations
    for key, value in SPARK_CONFIGS.items():
        if key == "spark.driver.maxResultSize":
            builder = builder.config(key, max_result_size)
        else:
            builder = builder.config(key, value)

    # Add JDBC driver if needed
    if os.path.exists(JDBC_DRIVER_PATH):
        builder = builder.config("spark.jars", JDBC_DRIVER_PATH)

    return builder.getOrCreate()


def get_review_schema():
    """
    Create and return the schema for review data.

    Returns:
        StructType: Schema for review data
    """
    schema = StructType()

    for field in REVIEW_SCHEMA_FIELDS:
        field_type = None
        if field["type"] == "string":
            field_type = StringType()
        elif field["type"] == "integer":
            field_type = IntegerType()
        elif field["type"] == "timestamp":
            field_type = TimestampType()

        if field_type:
            schema = schema.add(field["name"], field_type, field["nullable"])

    return schema


def validate_json(input_file, sample_size=10):
    """
    Validate if the JSON file is well-formed.

    Args:
        input_file: Path to the JSON file
        sample_size: Number of lines to check

    Returns:
        tuple: (is_valid, is_array, is_line_delimited, sample_structure)
    """
    logger.info(f"Validating JSON file format: {input_file}")

    try:
        # Try to determine if it's a JSON array or line-delimited JSON
        with open(input_file, 'r') as f:
            first_char = f.read(1).strip()

        is_array = first_char == '['
        is_line_delimited = first_char == '{'

        # Read a sample of lines for validation
        sample_structure = None
        valid_count = 0

        with open(input_file, 'r') as f:
            if is_array:
                # For JSON arrays, we need special handling
                logger.info("File appears to be a JSON array")
                try:
                    # Just try to load a small part of the file
                    with open(input_file, 'r') as f:
                        data = json.loads(f.read(1000000))  # Try to read first 1MB
                        if isinstance(data, list) and len(data) > 0:
                            sample_structure = data[0]
                            valid_count = 1
                except:
                    pass
            else:
                # For line-delimited JSON
                logger.info("File appears to be line-delimited JSON")
                for i, line in enumerate(f):
                    if i >= sample_size:
                        break

                    try:
                        parsed = json.loads(line.strip())
                        if not sample_structure and isinstance(parsed, dict):
                            sample_structure = parsed
                        valid_count += 1
                    except:
                        continue

        is_valid = valid_count > 0
        logger.info(f"JSON validation: valid={is_valid}, array={is_array}, line_delimited={is_line_delimited}")
        return (is_valid, is_array, is_line_delimited, sample_structure)

    except Exception as e:
        logger.error(f"Error validating JSON: {e}")
        return (False, False, False, None)


def convert_to_line_delimited(input_file, output_file):
    """
    Convert a JSON array file to line-delimited JSON.

    Args:
        input_file: Path to JSON array file
        output_file: Path to save line-delimited output

    Returns:
        bool: Success status
    """
    logger.info(f"Converting JSON array to line-delimited format")

    try:
        with open(input_file, 'r') as f:
            # Read the first line to check for array start
            first_line = f.readline().strip()
            if not first_line.startswith('['):
                logger.error("File does not start with array '['")
                return False

            # Reset to beginning
            f.seek(0)

            # Use the json module to load the array
            data = json.load(f)

        if not isinstance(data, list):
            logger.error("JSON file did not contain an array")
            return False

        # Write each array element as a separate JSON line
        with open(output_file, 'w') as f:
            for item in data:
                f.write(json.dumps(item) + '\n')

        logger.info(f"Conversion complete. Wrote {len(data)} records to {output_file}")
        return True

    except Exception as e:
        logger.error(f"Error converting JSON array: {e}")
        return False


def split_json_file(input_file, output_dir, chunk_size_mb=100):
    """
    Split a large JSON file into smaller chunks.

    Args:
        input_file: Path to the large JSON file
        output_dir: Directory to save the chunks
        chunk_size_mb: Approximate size of each chunk in MB

    Returns:
        List of chunk file paths
    """
    logger.info(f"Splitting JSON file {input_file} into chunks of approximately {chunk_size_mb}MB each")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Validate JSON format
    is_valid, is_array, is_line_delimited, sample = validate_json(input_file)

    if not is_valid:
        logger.error("JSON file validation failed")
        raise ValueError("Invalid JSON file format")

    # If it's a JSON array, convert to line-delimited first
    if is_array:
        temp_line_delimited = f"{os.path.dirname(input_file)}/temp_line_delimited.json"
        logger.info(f"Converting JSON array to line-delimited format: {temp_line_delimited}")

        if convert_to_line_delimited(input_file, temp_line_delimited):
            input_file = temp_line_delimited
        else:
            logger.error("Failed to convert JSON array to line-delimited format")
            raise ValueError("JSON conversion failed")

    # Get total file size
    file_size = os.path.getsize(input_file)
    file_size_mb = file_size / (1024 * 1024)
    logger.info(f"Input file size: {file_size_mb:.2f} MB")

    # Calculate number of lines per chunk (approximation)
    chunk_bytes = chunk_size_mb * 1024 * 1024

    # Initialize counters and chunk file list
    chunk_files = []
    total_chunks = 0

    try:
        # Use Unix split command for more efficient splitting if available
        if os.name != 'nt':  # Not Windows
            try:
                # Create a temporary split directory
                split_dir = f"{output_dir}/temp_split"
                os.makedirs(split_dir, exist_ok=True)

                # Split the file using Unix split
                chunk_prefix = f"{split_dir}/chunk_"
                subprocess.run(
                    f"split -b {chunk_bytes} -d {input_file} {chunk_prefix}",
                    shell=True,
                    check=True
                )

                # Get list of chunks
                raw_chunks = [f"{split_dir}/{f}" for f in os.listdir(split_dir) if f.startswith("chunk_")]
                raw_chunks.sort()

                # Process each raw chunk into valid JSON files
                for i, chunk in enumerate(raw_chunks):
                    # For line-delimited JSON, ensure each chunk has complete lines
                    chunk_output = f"{output_dir}/chunk_{i:03d}.json"

                    with open(chunk_output, 'w') as outfile:
                        with open(chunk, 'r') as infile:
                            for line in infile:
                                line = line.strip()
                                if line:  # Skip empty lines
                                    # Try to validate as JSON
                                    try:
                                        json.loads(line)
                                        outfile.write(line + '\n')
                                    except json.JSONDecodeError:
                                        logger.warning(f"Skipping invalid JSON line in chunk {i}")

                    chunk_files.append(chunk_output)
                    total_chunks += 1

                # Clean up temporary split directory
                shutil.rmtree(split_dir)

                # Clean up temporary line-delimited file if created
                if is_array and os.path.exists(temp_line_delimited):
                    os.remove(temp_line_delimited)

            except Exception as e:
                logger.error(f"Error using Unix split: {e}")
                raise
        else:
            # Manual splitting for Windows or if Unix split fails
            logger.info("Using Python-based file splitting")

            chunk_num = 0
            current_chunk_size = 0
            current_lines = []

            with open(input_file, 'r') as f:
                for line in f:
                    stripped_line = line.strip()
                    if not stripped_line:
                        continue

                    try:
                        # Validate JSON
                        json.loads(stripped_line)

                        line_size = len(stripped_line.encode('utf-8'))
                        current_lines.append(stripped_line + '\n')
                        current_chunk_size += line_size

                        if current_chunk_size >= chunk_bytes:
                            # Write current chunk
                            chunk_file = f"{output_dir}/chunk_{chunk_num:03d}.json"
                            with open(chunk_file, 'w') as chunk_f:
                                chunk_f.writelines(current_lines)

                            chunk_files.append(chunk_file)
                            total_chunks += 1
                            chunk_num += 1

                            # Reset for next chunk
                            current_lines = []
                            current_chunk_size = 0
                    except json.JSONDecodeError:
                        logger.warning("Skipping invalid JSON line")
                        continue

            # Write the last chunk if there's any data left
            if current_lines:
                chunk_file = f"{output_dir}/chunk_{chunk_num:03d}.json"
                with open(chunk_file, 'w') as chunk_f:
                    chunk_f.writelines(current_lines)

                chunk_files.append(chunk_file)
                total_chunks += 1

            # Clean up temporary line-delimited file if created
            if is_array and os.path.exists(temp_line_delimited):
                os.remove(temp_line_delimited)

    except Exception as e:
        logger.error(f"Error splitting file: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

    logger.info(f"Split complete. Created {total_chunks} chunks.")
    return chunk_files


def process_json_to_parquet(spark, input_path, output_path, salt_column="id", new_columns=None):
    """
    Process a JSON file and save as Parquet.

    Args:
        spark: Spark session
        input_path: Path to input JSON file
        output_path: Path to save the processed data as Parquet
        salt_column: Column to use for salting
        new_columns: Dictionary of new column names and values to add

    Returns:
        Processing time in seconds
    """
    start_time = time.time()

    # Create a minimal schema to prevent corrupt record issues
    base_schema = StructType().add("_temp", StringType(), True)

    # Read JSON file
    logger.info(f"Processing chunk: {input_path}")
    try:
        # First read with permissive mode to handle corrupt records
        df = spark.read \
            .option("multiline", "false") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .schema(base_schema) \
            .json(input_path)

        # Cache the DataFrame to avoid re-reading
        df.cache()

        # Check if we have any data
        if df.count() == 0:
            logger.warning(f"No valid records found in {input_path}")
            return 0

        # Drop the temporary column and infer the actual schema
        df = spark.read \
            .option("multiline", "false") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .json(input_path)

        # Filter out corrupt records if they exist
        if "_corrupt_record" in df.columns:
            df = df.filter(col("_corrupt_record").isNull())
            # Drop the corrupt record column
            df = df.drop("_corrupt_record")

        # Check for required columns
        missing_columns = [c for c in REQUIRED_RESTAURANT_COLUMNS if c not in df.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return 0

        # Check for nulls in key columns
        for col_name in REQUIRED_RESTAURANT_COLUMNS:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                logger.warning(f"Column '{col_name}' has {null_count} nulls")

        # Drop duplicates based on 'id'
        before_dedup = df.count()
        df = df.dropDuplicates(["id"])
        after_dedup = df.count()
        if before_dedup != after_dedup:
            logger.warning(f"Removed {before_dedup - after_dedup} duplicate records based on 'id'")

        logger.info(f"✅ Final record count after integrity checks: {after_dedup}")

        # Add new columns if specified
        if new_columns:
            for col_name, col_value in new_columns.items():
                df = df.withColumn(col_name, lit(col_value))

        # Add processing timestamp column
        df = df.withColumn("processing_timestamp", current_timestamp())

        # Add salting with ID if column exists
        if salt_column in df.columns:
            df = df.withColumn("salt", sha2(concat(col(salt_column), lit(SALT_KEY)), 256))
        else:
            # Try to find a suitable ID column
            possible_id_columns = [c for c in df.columns if 'id' in c.lower()]
            if possible_id_columns:
                alt_column = possible_id_columns[0]
                logger.info(f"Using alternative column for salting: {alt_column}")
                df = df.withColumn("salt", sha2(concat(col(alt_column), lit(SALT_KEY)), 256))
            else:
                logger.warning("No suitable ID column found for salting. Adding row number as salt.")
                df = df.withColumn("row_id", monotonically_increasing_id())
                df = df.withColumn("salt", sha2(concat(col("row_id"), lit(SALT_KEY)), 256))

        # Save to Parquet
        df.write.mode("append").parquet(output_path)

        processing_time = time.time() - start_time
        logger.info(f"Processed chunk in {processing_time:.2f} seconds")
        return processing_time

    except Exception as e:
        logger.error(f"Error processing chunk {input_path}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 0


def process_all_chunks(spark, chunk_files, output_path, salt_column="id", new_columns=None):
    """
    Process all JSON chunks and combine into a single Parquet output.

    Args:
        spark: Spark session
        chunk_files: List of chunk file paths
        output_path: Path to save the combined processed data
        salt_column: Column to use for salting
        new_columns: Dictionary of new column names and values to add
    """
    # Create output directory
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    total_start_time = time.time()
    total_processing_time = 0
    success_count = 0

    for i, chunk_file in enumerate(chunk_files):
        logger.info(f"Processing chunk {i + 1}/{len(chunk_files)}: {chunk_file}")

        try:
            # Process each chunk
            processing_time = process_json_to_parquet(
                spark,
                chunk_file,
                output_path,
                salt_column,
                new_columns
            )

            if processing_time > 0:
                total_processing_time += processing_time
                success_count += 1

            spark.catalog.clearCache()

        except Exception as e:
            logger.error(f"Error processing chunk {chunk_file}: {e}")
            # Continue with next chunk

    total_time = time.time() - total_start_time
    logger.info(f"All chunks processed. Total time: {total_time:.2f} seconds")
    logger.info(f"Pure processing time: {total_processing_time:.2f} seconds")
    logger.info(f"Successfully processed {success_count}/{len(chunk_files)} chunks")

    # Try to read back and verify the output
    try:
        output_df = spark.read.parquet(output_path)
        row_count = output_df.count()
        logger.info(f"Total rows in output Parquet: {row_count}")
        logger.info("Sample of output data:")
        output_df.show(5, truncate=False)
    except Exception as e:
        logger.warning(f"Could not verify output: {e}")


def cleanup_chunks(chunk_files, delete=True):
    """
    Clean up temporary chunk files.

    Args:
        chunk_files: List of chunk file paths
        delete: Whether to delete the files or just log
    """
    if delete:
        logger.info(f"Cleaning up {len(chunk_files)} temporary chunk files")
        for chunk_file in chunk_files:
            try:
                os.remove(chunk_file)
            except Exception as e:
                logger.warning(f"Could not delete {chunk_file}: {e}")
    else:
        logger.info(f"Keeping {len(chunk_files)} chunk files for inspection")


def flatten_json(input_file, output_restaurant_file, output_menu_file):
    """
    Flatten a nested JSON file into separate restaurant and menu files.

    Args:
        input_file: Path to input nested JSON file
        output_restaurant_file: Path to output flattened restaurant file
        output_menu_file: Path to output flattened menu file

    Returns:
        tuple: (restaurant_count, menu_item_count, duration, memory_mb)
    """
    # Create output dir if it doesn't exist
    os.makedirs(os.path.dirname(output_restaurant_file), exist_ok=True)

    restaurant_count = 0
    menu_item_count = 0

    start_time = time.time()
    process = psutil.Process(os.getpid())

    with open(input_file, 'r') as infile:
        logger.info(f"🔍 Loading data from {input_file}...")
        raw_data = json.load(infile)

    logger.info("⚙️ Flattening data...")

    with open(output_restaurant_file, 'w') as rest_out, open(output_menu_file, 'w') as menu_out:
        for city, city_info in tqdm(raw_data.items(), desc="Processing cities"):
            city_link = city_info.get("link")
            restaurants = city_info.get("restaurants", {})

            for rest_id, rest_data in restaurants.items():
                # Flat restaurant-level record
                flat_rest = {
                    "id": rest_id,
                    "name": rest_data.get("name"),
                    "city": city,
                    "rating": rest_data.get("rating"),
                    "rating_count": rest_data.get("rating_count"),
                    "cost": rest_data.get("cost"),
                    "cuisine": rest_data.get("cuisine"),
                    "lic_no": rest_data.get("lic_no"),
                    "link": city_link,
                    "address": rest_data.get("address")
                }
                rest_out.write(json.dumps(flat_rest) + "\n")
                restaurant_count += 1

                # Optional: Flatten menu
                menu = rest_data.get("menu", {})
                for category, items in menu.items():
                    for dish_name, details in items.items():
                        flat_dish = {
                            "restaurant_id": rest_id,
                            "restaurant_name": rest_data.get("name"),
                            "city": city,
                            "category": category,
                            "dish": dish_name,
                            "price": details.get("price"),
                            "veg_or_non_veg": details.get("veg_or_non_veg")
                        }
                        menu_out.write(json.dumps(flat_dish) + "\n")
                        menu_item_count += 1

    end_time = time.time()
    duration = end_time - start_time
    memory_mb = process.memory_info().rss / (1024 * 1024)

    logger.info("\n✅ Flattening Complete!")
    logger.info(f"🍽 Restaurants: {restaurant_count}")
    logger.info(f"🥘 Menu Items  : {menu_item_count}")
    logger.info(f"⏱️ Duration    : {duration:.2f} seconds")
    logger.info(f"🧠 Peak Memory : {memory_mb:.2f} MB")

    return (restaurant_count, menu_item_count, duration, memory_mb)