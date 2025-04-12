from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Init Spark
spark = SparkSession.builder.appName("StreamProcessor").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Define schema manually
schema = StructType([
    StructField("Id", IntegerType(), True),  # Id is INTEGER
    StructField("ProductId", StringType(), True),
    StructField("UserId", StringType(), True),
    StructField("ProfileName", StringType(), True),
    StructField("HelpfulnessNumerator", IntegerType(), True),
    StructField("HelpfulnessDenominator", IntegerType(), True),
    StructField("Score", IntegerType(), True),
    StructField("Time", IntegerType(), True),
    StructField("Summary", StringType(), True),
    StructField("Text", StringType(), True),
    StructField("ingest_time", TimestampType(), True)
])

# Streaming read from parquet_output with the defined schema
df_stream = spark.readStream.schema(schema).parquet("parquet_output/")

# Add current timestamp
df_enriched = df_stream.withColumn("read_timestamp", current_timestamp())

# Write stream to parquet_stream
query = df_enriched.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoint/stream_checkpoint") \
    .option("path", "parquet_stream/") \
    .start()

query.awaitTermination()
