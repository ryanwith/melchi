import os
from snowflake.connector import connect, ProgrammingError # type: ignore
from config import get_source_warehouse_config, get_cdc_table_config
from cdc.snowflake import snowflake_setup_all_cdc, snowflake_run_cdc

def create_connection():
    print("Creating connection")
    source_config = get_source_warehouse_config()
    if source_config["warehouse_type"] == "snowflake":
        """Creates a connection to the Snowflake database."""
        print("Creating connection to snowflake database")

        return connect(
                user=source_config["account_user"],
                password=source_config["account_password"],
                account=source_config["account_identifier"]
            )

def setup_cdc_tables(cursor):
    print("Setting up CDC tables")
    source_config = get_source_warehouse_config()
    if source_config["warehouse_type"] == "snowflake":    
        snowflake_setup_all_cdc(cursor, get_cdc_table_config())

def run_cdc(cursor):
    print("Initiating CDC")
    source_config = get_source_warehouse_config()
    if source_config["warehouse_type"] == "snowflake":    
        snowflake_run_cdc(cursor, get_cdc_table_config())

def execute_array_of_queries(cursor, queries):
    print("Begin executing queries")
    try:
        for query in queries:
            cursor.execute(query)
    except ProgrammingError as e:
        print(f"Failed to execute query: {query}")
        print(f"Error: {str(e)}")
    # Optionally, you might want to add a 'finally' block if there are cleanup actions to perform
    finally:
        print("Finished executing queries.")

