from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from utils import datetime_utils, db_utils, spark_utils, get_orders_by_date_range
from delta.tables import DeltaTable

def orders_etl_job(spark: SparkSession, start_date: str, end_date:str):

    table_name = "orders"

    schema = db_utils.get_metadata(table_name)["schema"]
    delta_table_path = db_utils.get_metadata(table_name)["delta_table_path"]

    # Check if the Delta table exists
    if DeltaTable.isDeltaTable(spark, delta_table_path):
        delta_table = DeltaTable.forPath(spark, delta_table_path)
    else:
        raise Exception("Delta Table does NOT exists!")

    new_data = get_orders_by_date_range.collect_order_data_with_metafields(start_date, end_date)

    if new_data == []:
        print("There is no change, hence df is empty. Nothing to write!")
        return

    new_df = spark.createDataFrame(new_data)

    # Cast the DataFrame to the desired schema
    new_df_casted = db_utils.cast_to_schema(new_df, schema)
    new_df_casted.printSchema()
    new_df_casted.show()

    # Perform the merge operation
    print("Performing Writing Operation")
    delta_table.alias("tgt").merge(
        new_df_casted.alias("src"),
        "tgt.order_id = src.order_id"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()

    print("Writing Operation is completed")

    db_utils.run_glue_crawler("ordersCrawler")
    print("Crawler run is completed")

curr_date = datetime_utils.get_current_local_date()
spark = spark_utils.create_spark_session()
orders_etl_job(spark, curr_date, curr_date)
