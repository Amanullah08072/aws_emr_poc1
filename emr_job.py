from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_date, year, month

spark = SparkSession.builder \
    .appName("Clickstream_Scale_Optimization") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Enforce schema to avoid expensive data scans across 5 GB
clickstream_schema = StructType([
    StructField("click_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("action_type", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("session_duration_sec", IntegerType(), True),
    StructField("purchase_amount", DoubleType(), True),
    StructField("http_status", IntegerType(), True)
])

print("Reading raw 5 GB Clickstream text logs from S3...")
raw_df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(clickstream_schema) \
    .load("s3://raw-zone-poc1/Input/user_activity_logs.csv")

# Parse transaction dates
processed_df = raw_df \
    .withColumn("event_date", to_date(col("event_timestamp"))) \
    .withColumn("event_year", year(col("event_date"))) \
    .withColumn("event_month", month(col("event_date")))

# Isolate clean data boundaries and drop corrupted system records
clean_analytics_df = processed_df.filter(
    (col("event_year") == 2026) & 
    (col("http_status") == 200) & 
    (col("product_category") != "CORRUPTED_EVENT")
).select(
    col("click_id"),
    col("session_id"),
    col("user_id"),
    col("product_category").alias("category"),
    col("action_type").alias("action"),
    col("device_type").alias("device"),
    col("purchase_amount").alias("revenue"),
    col("event_date"),
    col("event_month")
)

print("Writing clean partitioned Parquet to the Data Lake...")
clean_analytics_df.write \
    .mode("overwrite") \
    .partitionBy("event_month", "category") \
    .parquet("s3://gold-zone-poc1/clickstream_analytics/")

print("Spark Pipeline Succeeded!")
spark.stop()