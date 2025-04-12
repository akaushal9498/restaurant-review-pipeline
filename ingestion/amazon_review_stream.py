# File: stream_processor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Init Spark
spark = SparkSession.builder.appName("StreamProcessor").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# You can extract schema using a one-time batch read
sample_df = spark.read.parquet("parquet_output/")
schema = sample_df.schema

# Streaming read from parquet_output
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
