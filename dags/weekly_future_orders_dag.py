from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils import datetime_utils, spark_utils
from jobs.orders_etl_job import orders_etl_job
from jobs.dim_customers_scd import dim_customers_scd_etl_job
from jobs.weekly_streamline_orders import weekly_streamline_orders_etl_job
from jobs.future_orders import future_orders_etl_job
import pytz

# Get the current date
curr_date = datetime_utils.get_current_local_date()
runtime_param_run_type = "custom"
start_date = "2024-06-20"
end_date = "2024-06-25"
if runtime_param_run_type == "default":
    start_date = curr_date
    end_date = curr_date
    
spark = spark_utils.create_spark_session()

def curr_orders_etl_job(start_date, end_date):
    
    return orders_etl_job(spark, start_date, end_date)

def curr_dim_customers_scd(start_date, end_date):
    
    return dim_customers_scd_etl_job(spark, start_date, end_date)

# Default arguments for the DAG
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 18),
    'email': ['abc@xyz.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
@dag(
    dag_id="weekly_future_orders_dag",
    default_args=default_args,
    description="This DAG runs an ETL process for weekly future orders.",
    schedule_interval="@daily",
    catchup=False,
    tags=["bmd"],
)
def weekly_future_orders_dag():
    start = EmptyOperator(task_id="start", trigger_rule="all_done")

    task_curr_orders_etl_job = PythonOperator(
        task_id="task_curr_orders_etl_job",
        python_callable=curr_orders_etl_job,
        op_args=[curr_date, curr_date]
    )

    task_curr_dim_customers_scd = PythonOperator(
        task_id="task_curr_dim_customers_scd",
        python_callable=curr_dim_customers_scd,
        op_args=[curr_date, curr_date]
    )

    task_weekly_streamline_orders_etl_job= PythonOperator(
        task_id="task_weekly_streamline_orders_etl_job",
        python_callable=weekly_streamline_orders_etl_job
    )

    task_future_orders_etl_job = PythonOperator(
        task_id="task_future_orders_etl_job",
        python_callable=future_orders_etl_job
    )

    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    start >> [task_curr_orders_etl_job,task_curr_dim_customers_scd] >> [task_weekly_streamline_orders_etl_job,task_future_orders_etl_job] >> end

# Instantiate the DAG
dag_instance = weekly_future_orders_dag()

spark.stop()
