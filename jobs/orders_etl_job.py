from pyspark.sql import SparkSession
from get_orders_by_date_range import collect_order_data

# Initialize Spark session with Delta Lake support
spark = SparkSession.builder \
    .appName("DeltaLakeSQLExample") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Path to Delta table
delta_table_path = "s3a://devbmdanalayticsdata/silver/orders"

# Load Delta table into a DataFrame
delta_df = spark.read.format("delta").load(delta_table_path)

# Create a temporary view for the existing Delta table
delta_df.createOrReplaceTempView("orders")
spark.sql("SELECT * FROM orders").show()

start_date = "2023-05-27"
end_date = "2023-05-27"
new_data = collect_order_data(start_date, end_date)

new_df = spark.createDataFrame(new_data)
new_df.show()

# # Create a temporary view for the new data
# new_df.createOrReplaceTempView("new_orders")

# # Perform the upsert operation using SQL

# # Use a SQL query to delete matched records
# spark.sql("""
# DELETE FROM orders
# USING new_orders
# WHERE orders.order_id = new_orders.order_id
# """)

# # Use a SQL query to insert new and updated records
# spark.sql("""
# INSERT INTO orders
# SELECT * FROM new_orders
# """)

# # Verify the result
# updated_df = spark.read.format("delta").load(delta_table_path)
# updated_df.show()

# Stop the Spark session
spark.stop()
