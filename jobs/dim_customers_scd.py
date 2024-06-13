from pyspark.sql import SparkSession
from get_customers_by_date_range import collect_customer_data
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, IntegerType
from utils import db_utils, spark_utils
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

# Initialize Spark session with Delta Lake support
spark = spark_utils.create_spark_session()

bucket_name = "devbmdanalayticsdata"
layer = "gold"
table_name = "dim_customers_scd"

# Path to Delta table
delta_table_path = f"s3a://{bucket_name}/{layer}/{table_name}"

# Define the schema for the order_info
schema = StructType([
    StructField("customer_id", LongType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("number_of_orders", IntegerType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("effective_start_date", TimestampType(), True),
    StructField("effective_end_date", TimestampType(), True)
])

# # Check if the Delta table exists
# if DeltaTable.isDeltaTable(spark, delta_table_path):
#     delta_table = DeltaTable.forPath(spark, delta_table_path)
# else:
#     raise Exception("Delta Table does NOT exists!")

start_date = "2024-05-28"
end_date = "2024-05-29"
new_data = collect_customer_data(start_date, end_date)

new_df = spark.createDataFrame(new_data) \
    .withColumn("effective_start_date", lit(None)) \
    .withColumn("effective_end_date", lit(None))

# Cast the DataFrame to the desired schema
new_df_casted = db_utils.cast_to_schema(new_df, schema)
new_df_casted.printSchema()
new_df_casted.show()

# # Perform the merge operation
# delta_table.alias("tgt").merge(
#     new_df_casted.alias("src"),
#     "tgt.order_id = src.order_id"
# ).whenMatchedUpdateAll(
# ).whenNotMatchedInsertAll(
# ).execute()

# Stop the Spark session
spark.stop()
