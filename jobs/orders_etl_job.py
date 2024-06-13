from pyspark.sql import SparkSession
from get_orders_by_date_range import collect_order_data
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from utils import db_utils, spark_utils
from delta.tables import DeltaTable

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

# Check if the Delta table exists
if DeltaTable.isDeltaTable(spark, delta_table_path):
    delta_table = DeltaTable.forPath(spark, delta_table_path)
else:
    raise Exception("Delta Table does NOT exists!")

start_date = "2024-01-01"
end_date = "2024-12-31"
new_data = collect_order_data(start_date, end_date)

new_df = spark.createDataFrame(new_data)

# Cast the DataFrame to the desired schema
new_df_casted = db_utils.cast_to_schema(new_df, schema)
new_df_casted.printSchema()
new_df_casted.show()

# Perform the merge operation
delta_table.alias("tgt").merge(
    new_df_casted.alias("src"),
    "tgt.order_id = src.order_id"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()

# Stop the Spark session
spark.stop()
