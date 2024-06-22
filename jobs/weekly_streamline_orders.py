from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from utils import db_utils, spark_utils, get_orders_by_date_range
from delta.tables import DeltaTable

def weekly_streamline_orders_etl_job(spark:SparkSession):

    # Table_name
    table_name = "weekly_streamline_orders"

    schema = db_utils.get_metadata(table_name)["schema"]
    delta_table_path = db_utils.get_metadata(table_name)["delta_table_path"]

    query = """
    WITH orders AS(
        SELECT 
            * 
        FROM 
            bmd.orders
        WHERE 
            financial_status = 'PAID' AND
            pickup_date > current_date 

    ), customers AS(
        SELECT 
            *
        FROM
            bmd.dim_customers_scd
        WHERE
            is_active = true
    )
    SELECT
        o.order_name,
        c.first_name,
        o.draft_type,
        o.theme,
        o.flavor,
        o.allergies,
        o.pickup_date
    FROM
        customers c
    JOIN    
        orders o
    ON
        c.customer_id = o.customer_id
    ORDER BY
        o.pickup_date
    """

    # Read the data from Redshift Spßectrum using the query
    weekly_streamline_orders = db_utils.run_query_from_redshift(spark, query)
    weekly_streamline_orders.write.format("delta").mode("overwrite").save(delta_table_path)
    db_utils.run_glue_crawler("weeklyStreamlineOrdersCrawler")