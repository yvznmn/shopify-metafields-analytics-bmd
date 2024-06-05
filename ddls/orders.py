from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType, LongType
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access the environment variables
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# Ensure the shop_name and access_token are loaded correctly
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    raise ValueError("AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY not found in environment variables.")

# Initialize Spark session with Delta Lake support
spark = SparkSession.builder \
    .appName("DeltaLakeCreateEmptyTable") \
    .config("spark.jars.packages", "io.delta:delta-spark:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
    # .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
    # .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
    # .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    # .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    # .getOrCreate()

# Define the schema for the Delta table
schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("order_name", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("processed_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("financial_status", TimestampType(), True),
    StructField("customer_id", LongType(), True),
    StructField("total_price", DecimalType(20, 4), True)
])

# Create an empty DataFrame with the defined schema
empty_df = spark.createDataFrame([], schema)

# Path to Delta table
delta_table_path = "s3a://devbmdanalayticsdata/silver/orders"

# Write the empty DataFrame to Delta format
empty_df.write.format("delta").mode("overwrite").save()

# # Verify by reading the empty Delta table
# df = spark.read.format("delta").load(delta_table_path)
# df.printSchema()
# df.show()

# Stop the Spark session
spark.stop()
