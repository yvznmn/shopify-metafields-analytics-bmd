
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType, DateType, BooleanType, StructType, StructField, StringType, TimestampType, IntegerType, LongType
import boto3

def create_delta_table(spark_session: SparkSession, delta_table_path: str, table_name: str, schema: StructType):
    """
    Create an empty Delta table with the specified schema.

    :param spark_session: The SparkSession to use.
    :param delta_table_path: The S3 path where the Delta table will be stored.
    :param table_name: The name of the Delta table to create.
    :param schema: The schema of the Delta table.
    """
    print(f"Creating an empty DataFrame with the schema: {schema}")
    # Create an empty DataFrame with the defined schema
    empty_df = spark_session.createDataFrame([], schema)

    print(f"Writing the empty DataFrame to Delta format at path: {delta_table_path}")
    # Write the empty DataFrame to Delta format
    empty_df.write.format("delta").save(delta_table_path)

    print(f"Creating Delta table '{table_name}' at location '{delta_table_path}'")
    # Create a Delta table from the saved path
    spark_session.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{delta_table_path}'")
    
    print(f"Delta table '{table_name}' created successfully.")

def show_create_delta_table(delta_table_path: str, table_name: str):

    print((f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{delta_table_path}'"))

def drop_delta_table(spark_session: SparkSession, delta_table_path: str, table_name: str):
    """
    Drops the specified Delta table if it exists.

    Parameters:
    - spark_session: SparkSession instance.
    - delta_table_path: Path to the Delta table.
    - table_name: Name of the Delta table.
    """
    try:
        # Drop the table
        spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
        print(f"Delta table '{table_name}' dropped successfully.")
        
    except Exception as e:
        print(f"An error occurred while dropping the Delta table: {e}")


# Function to cast DataFrame columns to specified schema
def cast_to_schema(df, schema):

    print(f"Casting dataframe to schema")
    for field in schema.fields:
        df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    print(f"Schema Casting is completed.")
    return df

def convert_schema_to_glue_format(schema):
    type_mapping = {
        'LongType': 'bigint',
        'StringType': 'string',
        'TimestampType': 'timestamp',
        'DoubleType': 'double'
    }

    print("Starting conversion of Spark StructType to Glue format...")

    glue_schema = []

    for field in schema.fields:
        field_type = type(field.dataType).__name__
        if field_type in type_mapping:
            glue_field = {
                'Name': field.name,
                'Type': type_mapping[field_type]
            }
            glue_schema.append(glue_field)

    print("Conversion completed. Resulting Glue schema:")

    return glue_schema

def create_glue_delta_table(glue: boto3.session.Session.client, database_name: str, schema: StructType, table_name: str, delta_table_path: str):

    glue_schema = convert_schema_to_glue_format(schema=schema)

    # Define the table schema
    table_input = {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': glue_schema,
            'Location': delta_table_path,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            }
        },
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'EXTERNAL': 'TRUE',
            'classification': 'delta',
            'delta.table': 'true'
        }
    }

    print("Glue table is being created")
    glue.create_table(
        DatabaseName=database_name,
        TableInput=table_input
    )
    print("Glue table is ready.")


def get_metadata(table_name):

    dim_customers_scd = StructType([
        StructField("customer_id", LongType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("number_of_orders", IntegerType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("effective_start_date", DateType(), True),
        StructField("effective_end_date", DateType(), True),
        StructField("is_active", BooleanType(), True),
    ])

    orders = StructType([
        StructField("order_id", LongType(), True),
        StructField("order_name", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("processed_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("financial_status", TimestampType(), True),
        StructField("customer_id", LongType(), True),
        StructField("total_price", DecimalType(20, 4), True)
    ])

    metadata = {
        "dim_customers_scd": {
            "schema": dim_customers_scd,
            "delta_table_path": "s3a://devbmdanalayticsdata/gold/dim_customers_scd"
        },
        "orders": {
            "schema": orders,
            "delta_table_path": "s3a://devbmdanalayticsdata/silver/orders"
        },
    }

    return metadata[table_name]