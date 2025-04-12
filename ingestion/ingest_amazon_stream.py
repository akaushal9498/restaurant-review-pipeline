# from pyspark.sql import SparkSession
#
# # Path to the downloaded SQLite JDBC driver JAR file
#
# # Initialize Spark session with the JDBC driver

#
# # Your SQLite JDBC URL and properties
# jdbc_url = "jdbc:sqlite:/Users/anupkaushal/PycharmProjects/restaurant-review-pipeline/data/database.sqlite"
# properties = {
#     "driver": "org.sqlite.JDBC"
# }
#
# # Read data from SQLite table
# df = spark.read.jdbc(url=jdbc_url, table="Reviews", properties=properties)
# df.show()

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import time

jdbc_driver_path = "/Users/anupkaushal/PycharmProjects/restaurant-review-pipeline/jar_files/sqlite-jdbc-3.49.1.0.jar"

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("JSON Processing") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.maxResultSize", "4g") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.default.parallelism", "20") \
    .config("spark.sql.files.ignoreCorruptFiles", "true") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

# SQLite JDBC config
db_path = "/Users/anupkaushal/PycharmProjects/restaurant-review-pipeline/data/database.sqlite"
jdbc_url = f"jdbc:sqlite:{db_path}"
table_name = "Reviews"

# Simulate streaming with chunks
chunk_size = 2000  # Estimate this based on your average row size to ~10 MB
offset = 0
chunk_id = 1

while True:
    print(f"Reading chunk {chunk_id}...")

    # Create JDBC query using subquery with LIMIT + OFFSET
    query = f"(SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset}) AS chunk"

    # Read chunk into Spark DataFrame
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", query) \
        .option("driver", "org.sqlite.JDBC") \
        .load()

    # Break if no more data
    if df.rdd.isEmpty():
        print("No more data to read.")
        break

    # Add current timestamp
    df = df.withColumn("ingest_time", current_timestamp())

    # Save as Parquet
    df.write.mode("append").parquet("parquet_output".format(chunk_id))

    print(f"Chunk {chunk_id} written.")

    # Sleep to simulate streaming delay
    time.sleep(5)

    # Update for next chunk
    offset += chunk_size
    chunk_id += 1

# Stop Spark session
spark.stop()

