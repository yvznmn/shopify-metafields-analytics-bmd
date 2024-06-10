
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col

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