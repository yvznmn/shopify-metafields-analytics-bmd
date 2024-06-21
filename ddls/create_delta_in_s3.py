from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, BooleanType, StructType, StructField, StringType, TimestampType, IntegerType, LongType
from dotenv import load_dotenv
import os
from utils import db_utils, spark_utils

# Initialize Spark session with Delta Lake support
spark = spark_utils.create_spark_session()
table_name = "future_orders"

schema = db_utils.get_metadata(table_name)["schema"]
delta_table_path = db_utils.get_metadata(table_name)["delta_table_path"]

# Create an empty DataFrame with the defined schema
empty_df = spark.createDataFrame([], schema)

# Write the empty DataFrame to Delta format
empty_df.write.format("delta").mode("overwrite").save(delta_table_path)

# Verify by reading the empty Delta table
df = spark.read.format("delta").load(delta_table_path)
df.printSchema()
df.show()

# Stop the Spark session
spark.stop()
