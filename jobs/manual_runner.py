from dim_customers_scd import dim_customers_scd_etl_job
from future_orders import future_orders_etl_job
from orders_etl_job import orders_etl_job
from weekly_streamline_orders import weekly_streamline_orders_etl_job
from utils import spark_utils
import time

start_date = "2024-06-20"
end_date = "2024-06-25"

spark = spark_utils.create_spark_session()

orders_etl_job(spark, start_date, end_date)
time.sleep(5)
dim_customers_scd_etl_job(spark, start_date, end_date)
time.sleep(5)
weekly_streamline_orders_etl_job(spark)
time.sleep(5)
future_orders_etl_job(spark)
time.sleep(5)

spark.stop()