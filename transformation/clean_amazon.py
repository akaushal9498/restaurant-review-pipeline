from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder.appName("CleanAmazonReviews").getOrCreate()

# Load streaming reviews from SQLite
df = (
    spark.read.format("jdbc")
         .option("url", "jdbc:sqlite:data/amazon_reviews.sqlite")
         .option("dbtable", "streaming_reviews")
         .option("driver", "org.sqlite.JDBC")
         .load()
)

# Basic cleaning
cleaned_df = (
    df.dropDuplicates(["Id"])
      .na.drop(subset=["Text", "Score"])
      .withColumn("ingestion_time", current_timestamp())
      .withColumnRenamed("Text", "review_text")
      .withColumnRenamed("Score", "rating")
)

# Save as Parquet
destination = "data/processed/streaming_reviews.parquet"
cleaned_df.write.mode("append").parquet(destination)

spark.stop()