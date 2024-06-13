from pyspark.sql import SparkSession
from get_orders_by_date_range import collect_order_data
from dotenv import load_dotenv
import os
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from utils import db_utils, spark_utils
import boto3

# Initialize Spark session with Delta Lake support
spark = spark_utils.create_spark_session()

# Path to Delta table
delta_table_path = "s3a://devbmdanalayticsdata/silver/orders"

# Table_name
table_name = "orders"

# Define the schema for the order_info
schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("order_name", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("processed_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("financial_status", StringType(), True),
    StructField("customer_id", LongType(), True),
    StructField("total_price", DoubleType(), True)
])

database_name = "dev"

# Initialize Glue client with region specified
glue = boto3.client('glue', region_name='us-east-2')

db_utils.create_glue_delta_table(glue=glue, 
                                 database_name=database_name, 
                                 schema=schema, 
                                 table_name=table_name, 
                                 delta_table_path=delta_table_path)

db_utils.create_delta_table(spark, delta_table_path, table_name, schema)

# # Stop the Spark session
spark.stop()
