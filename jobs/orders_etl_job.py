from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os


# Load environment variables from .env file
load_dotenv()

# Access the environment variables
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

spark = SparkSession.builder \
    .appName("BMDAnalytics") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1,org.apache.hadoop:hadoop-aws:3.2.0") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


from delta.tables import DeltaTable

# Path to Delta table
delta_table_path = "s3://devbmdanalayticsdata/silver/orders/"

# Load Delta table
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Example DataFrame for new data
new_data = [
    ("1", "order1", "2024-01-01", "completed", "John Doe", 150.0),  # Updated order
    ("3", "order3", "2024-01-03", "paid", "Alice Johnson", 300.0)   # New order
]
new_columns = ["order_id", "order_name", "created_at", "financial_status", "customer_name", "total_price"]
new_df = spark.createDataFrame(new_data, new_columns)
