from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.appName("CleanSwiggyData").getOrCreate()

# Load CSV version of Swiggy data (previously converted from JSON)
df = spark.read.csv("data/swiggy_restaurants.csv", header=True, inferSchema=True)

# Clean and normalize
df_clean = (
    df.dropDuplicates()
      .withColumnRenamed("name", "restaurant_name")
      .withColumnRenamed("address", "restaurant_address")
      .withColumn("listed_date", to_date(col("listed_date"), "yyyy-MM-dd"))
      .na.fill({"avg_rating": 0, "delivery_time": "N/A"})
)

df_clean.write.mode("overwrite").parquet("data/processed/swiggy_restaurants.parquet")

spark.stop()