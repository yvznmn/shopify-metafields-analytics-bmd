import dash
import dash_table
from dash import dcc, html
from pyspark.sql import SparkSession
from utils import spark_utils, db_utils
import json

# Initialize Spark session
spark = spark_utils.create_spark_session()

# Create a sample Spark DataFrame
query_weekly_streamline_orders = """
SELECT
    *
FROM
    bmd.weekly_streamline_orders
"""

query_future_orders = """
SELECT
    *
FROM
    bmd.future_orders
"""


spark_df_weekly_streamline_orders = db_utils.run_query_from_redshift(spark, query_weekly_streamline_orders)
spark_df_future_orders = db_utils.run_query_from_redshift(spark, query_future_orders)

# Convert Spark DataFrame to JSON
json_data_weekly_streamline_orders = spark_df_weekly_streamline_orders.toJSON().collect()
json_data_future_orders = spark_df_future_orders.toJSON().collect()

# Convert JSON data to Python dict
data_dicts_weekly_streamline_orders = [json.loads(row) for row in json_data_weekly_streamline_orders]
data_dicts_future_orders = [json.loads(row) for row in json_data_future_orders]

# Create a Dash app
app = dash.Dash(__name__)

# Define the layout
app.layout = html.Div([
    html.H3("Table: Weekly Streamline Orders"),
    dash_table.DataTable(
        id='weekly_streamline_orders',
        columns=[{"name": i, "id": i} for i in spark_df_weekly_streamline_orders.columns],
        data=data_dicts_weekly_streamline_orders,
    ),
    html.H3("Table: Future Orders"),
    dash_table.DataTable(
        id='future_orders',
        columns=[{"name": i, "id": i} for i in spark_df_future_orders.columns],
        data=data_dicts_future_orders,
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)

    spark.stop()
