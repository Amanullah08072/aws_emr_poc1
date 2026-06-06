import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

# 1. Instantiate the Core Glue & Distributed Spark Contexts
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("Extracting partition schemas from Glue Catalog table...")
# 2. Extract data directly from your catalog metadata layer
# This automatically handles mapping underlying S3 nested Parquet file structures
dynamic_frame_read = glueContext.create_dynamic_frame.from_catalog(
    database="poc1_db1", 
    table_name="clickstream_analytics"
)

# Convert the Glue DynamicFrame into an optimized Spark DataFrame 
df = dynamic_frame_read.toDF()

# Map the active DataFrame into an isolated virtual view for Spark SQL processing
df.createOrReplaceTempView("clickstream_analytics_view")

print("Executing relational PySpark SQL CTE Aggregations...")
# 3. Process your analytics logic across thousands of rows concurrently
transformed_df = spark.sql("""
    WITH CategoryMetrics AS (
        SELECT 
            category,
            COUNT(*) AS total_clicks,
            SUM(CASE WHEN action = 'checkout_complete' THEN 1 ELSE 0 END) AS total_sales,
            ROUND(SUM(CAST(revenue AS DOUBLE)), 2) AS gross_revenue
        FROM clickstream_analytics_view
        GROUP BY category
    ),
    PerformanceIndexing AS (
        SELECT 
            category,
            total_clicks,
            total_sales,
            gross_revenue,
            ROUND((CAST(total_sales AS DOUBLE) / NULLIF(total_clicks, 0)) * 100, 2) AS conversion_rate
        FROM CategoryMetrics
    )
    SELECT 
        category,
        total_clicks,
        total_sales,
        gross_revenue,
        conversion_rate,
        CURRENT_TIMESTAMP() AS data_sync_timestamp
    FROM PerformanceIndexing
""")

print("Streaming aggregated data frame metrics straight to Aurora PostgreSQL...")
# 4. Convert back to DynamicFrame and dump records using the JDBC network profile
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(transformed_df, glueContext, "transformed_df"),
    connection_type="postgresql",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "ecommerce_performance_summary",
        "connectionName": "aurora_postgres_conn"
    }
)

print("SUCCESS! Lakehouse data sync-load to Aurora PostgreSQL completed.")
job.commit()
