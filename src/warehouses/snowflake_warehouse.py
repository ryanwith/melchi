# src/warehouses/snowflake_warehouse.py

import snowflake.connector
import pandas as pd
from .abstract_warehouse import AbstractWarehouse
from ..utils.table_config import get_cdc_type
import re
from .type_mappings import TypeMapper


class SnowflakeWarehouse(AbstractWarehouse):
    """
    Snowflake warehouse implementation that handles data extraction and CDC operations.
    Manages connections, schema operations, and change tracking for Snowflake databases.
    """

    def __init__(self, config):
        super().__init__("snowflake")  # Initialize with the warehouse type
        self.config = config
        self.connection = None
        self.cursor = None

    # CONNECTION METHODS
    
    def connect(self, role = None):
        if role == None:
            role = self.config["role"]
        """Creates a cursor and sets the role and warehouse for operations."""
        connect_params = {
            'account': self.config['account'],
            'user': self.config['user'],
            'password': self.config['password']
        }
        self.connection = snowflake.connector.connect(**connect_params)
        self.cursor = self.connection.cursor()
        self.cursor.execute(f"USE ROLE {role};")
        self.cursor.execute(f"USE WAREHOUSE {self.config["warehouse"]};")

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None

    # TRANSACTION MANAGEMENT
    
    def begin_transaction(self):
        self.cursor.execute("BEGIN;")

    def commit_transaction(self):
        self.connection.commit()

    def rollback_transaction(self):
        self.connection.rollback()

    # SCHEMA AND TABLE MANAGEMENT
    
    def get_schema(self, table_info):
        # input: table_info dictionary containing keys database, schema, and table
        # output: array of schema dictionaries as provided in format_schema_row 
        if self.connection == None:
            raise ConnectionError("You have not established a connection to the database")
        elif self.cursor == None:
            raise ConnectionError("You do not have a valid cursor")
        
        self.cursor.execute(f"DESC TABLE {self.get_full_table_name(table_info)}")
        schema = []
        for row in self.cursor.fetchall():
            schema.append(self.format_schema_row(row))
        return schema

    def create_table(self, table_info, source_schema, target_schema):
        # Implementation for creating a table in Snowflake
        pass

    def get_full_table_name(self, table_info):
        database = table_info["database"]
        schema = table_info["schema"]
        table = table_info["table"]
        return f"{database}.{schema}.{table}"
    
    def replace_existing_tables(self):
        replace_existing = self.config.get("replace_existing", False)
        if replace_existing == True:
            return True
        else:
            return False

    def format_schema_row(self, row):
        # input: row of the schema as provided in a cursor
        return {
            "name": row[0],
            "type": row[1],
            "nullable": True if row[3] == "Y" else False,
            "default_value": row[4],
            "primary_key": True if row[5] == "Y" else False
        }

    # CHANGE TRACKING MANAGEMENT
    
    def get_change_tracking_schema_full_name(self):
        return f"{self.config["change_tracking_database"]}.{self.config["change_tracking_schema"]}"
    
    def setup_environment(self, tables_to_transfer = None):
        if self.config["warehouse_role"] == "TARGET":
            raise NotImplementedError(f"Snowflake is not yet supported as a target environment")
        elif self.config["warehouse_role"] == "SOURCE":
            self.setup_source_environment(tables_to_transfer)
        else:
            raise ValueError(f"Unknown warehouse role: {self.config["warehoues_role"]}")

    def setup_source_environment(self, tables_to_transfer):
        """Creates streams and CDC tables for each table to be transferred."""
        if tables_to_transfer == []:
            raise Exception("No tables to transfer found")

        problems = self.find_problems(tables_to_transfer)
        if problems:
            raise ValueError(f"The following problems were found:\n{'\n'.join(problems)}")

        for table_info in tables_to_transfer:
            cdc_type = table_info.get("cdc_type", "FULL_REFRESH").upper()
            if cdc_type in ("STANDARD_STREAM", "APPEND_ONLY_STREAM"):
                self.create_stream_objects(table_info)

    def setup_target_environment(self):
        pass

    def get_stream_name(self, table_info):
        """Returns the stream name for the given table."""
        database = table_info["database"]
        schema = table_info["schema"]
        table = table_info["table"]
        return f"{self.get_change_tracking_schema_full_name()}.{database}${schema}${table}"
    
    def get_stream_processing_table_name(self, table_info):
        """Returns the processing table name for the given table's stream."""
        database = table_info["database"]
        schema = table_info["schema"]
        table = table_info["table"]
        return f"{self.get_change_tracking_schema_full_name()}.{database}${schema}${table}_processing"

    def create_stream_objects(self, table_info):
        """
        Creates a snowflake stream and permanent table for CDC tracking.
        Generates unique names for stream and processing table.
        """
        stream_name = self.get_stream_name(table_info)
        table_name = f"{table_info["database"]}.{table_info["schema"]}.{table_info["table"]}"
        cdc_type = table_info["cdc_type"]
        append_only_statement = f"APPEND_ONLY = {"TRUE" if cdc_type == "APPEND_ONLY_STREAM" else "FALSE"}"
        stream_processing_table = f"{stream_name}_processing"
        if self.replace_existing_tables() == True:
            create_query = f"CREATE OR REPLACE TABLE {stream_processing_table} LIKE {table_name};"
            create_stream_query = f"CREATE OR REPLACE STREAM {stream_name} ON TABLE {table_name} SHOW_INITIAL_ROWS = true {append_only_statement}"
        else:
            create_query = f"CREATE TABLE {stream_processing_table} IF NOT EXISTS LIKE {table_name};"
            create_stream_query = f"CREATE STREAM IF NOT EXISTS {stream_name} ON TABLE {table_name} SHOW_INITIAL_ROWS = true {append_only_statement}"
        create_stream_processing_table_queries = [
            create_stream_query,
            create_query,
            f"ALTER TABLE {stream_processing_table} ADD COLUMN IF NOT EXISTS\"METADATA$ACTION\" varchar;",
            f"ALTER TABLE {stream_processing_table} ADD COLUMN IF NOT EXISTS \"METADATA$ISUPDATE\" varchar;",
            f"ALTER TABLE {stream_processing_table} ADD COLUMN IF NOT EXISTS \"METADATA$ROW_ID\" varchar;"
        ]
        for query in create_stream_processing_table_queries:
            self.cursor.execute(query)


    # DATA SYNCHRONIZATION
    
    def sync_table(self, table_info, updates_dict):
        raise NotImplementedError("Snowflake is not yet supported as a target")

    def cleanup_source(self, table_info):
        """Removes processed records from the stream processing table."""
        if get_cdc_type(table_info) in ("APPEND_ONLY_STREAM", "STANDARD_STREAM"):
            stream_processing_table_name = self.get_stream_processing_table_name(table_info)
            try:
                self.cursor.execute(f"TRUNCATE TABLE {stream_processing_table_name}")
            except Exception as e:
                # Check for table not found error - Snowflake error code 002003
                if "002003" in str(e) or "does not exist" in str(e).lower():
                    table_name = self.get_full_table_name(table_info)
                    raise Exception(
                        f"Stream processing table not found for {table_name}. "
                        f"This typically means the initial setup was not completed. "
                        f"Please run 'python main.py setup' to set up change tracking for this table."
                    ) from None
                else:
                    # Re-raise any other exceptions
                    raise

    def get_updates(self, table_info):
        """
        Retrieves CDC data from stream table and returns changes in a dictionary format.
        For standard streams: returns both delete and insert records
        For append-only streams: returns only insert records
        For full refresh: returns only insert records
        """
        cdc_type = get_cdc_type(table_info)
        updates_dict = {
            "records_to_delete": None,
            "records_to_insert": None
        }

        if cdc_type in ("APPEND_ONLY_STREAM", "STANDARD_STREAM"):
            stream_processing_table_name = self.get_stream_processing_table_name(table_info)
            stream_name = self.get_stream_name(table_info)
            
            # Load stream data into processing table
            self.cursor.execute(f"INSERT INTO {stream_processing_table_name} SELECT * FROM {stream_name};")
            
            if cdc_type == "STANDARD_STREAM":
                # For standard streams, get primary keys of records to delete
                primary_keys = self.get_primary_keys(table_info)
                if primary_keys == []:
                    primary_keys = ["METADATA$ROW_ID as MELCHI_ROW_ID"]
                pk_columns = ", ".join(primary_keys)
                delete_query = f"""
                    SELECT {pk_columns}
                    FROM {stream_processing_table_name}
                    WHERE "METADATA$ACTION" = 'DELETE'
                """
                updates_dict["records_to_delete"] = self.get_data_as_df(delete_query)

            # Get records to insert (for both stream types)
            insert_query = f"""
                SELECT * 
                FROM {stream_processing_table_name}
                WHERE "METADATA$ACTION" = 'INSERT'
            """
            insert_df = self.get_data_as_df(insert_query)
            insert_df.rename(columns={
                "METADATA$ROW_ID": "MELCHI_ROW_ID",
                "METADATA$ACTION": "MELCHI_METADATA_ACTION"
            }, inplace=True)
            updates_dict["records_to_insert"] = insert_df

        elif cdc_type == "FULL_REFRESH":
            # For full refresh, we only need insert records
            updates_dict["records_to_insert"] = self.get_data_as_df(
                f"SELECT * FROM {self.get_full_table_name(table_info)}"
            )

        return updates_dict


    # UTILITY METHODS
    
    def execute_query(self, query_text, return_results = False):
        """Executes a query and optionally returns results."""
        self.cursor.execute(query_text)
        if return_results:
            return self.cursor.fetchall()

    def get_data_as_df(self, query_text):
        """
        Executes a query and returns results as batches of DataFrames.
        Uses configured batch size if provided, otherwise uses Snowflake's default batching.
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query_text)
            print("Query executed successfully")
            try:
                batch_size = self.config.get('batch_size')
                if batch_size:
                    return cursor.fetch_pandas_batches(batch_size=batch_size)
                return cursor.fetch_pandas_batches()
            except Exception as e:
                print(f"Error fetching as DataFrame batches: {str(e)}")
                raise
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            raise

    def get_primary_keys(self, table_info):
        schema = self.get_schema(table_info)
        return sorted([col['name'] for col in schema if col['primary_key']])

    def generate_source_sql(self, tables):
        role = self.config['role']
        warehouse = self.config['warehouse']

        create_change_tracking_schema_statement = [
            "--This command creates the change tracking schema.  Not required if it already exists.",
            f"CREATE SCHEMA IF NOT EXISTS {self.get_change_tracking_schema_full_name()};",
            "\n"
        ]

        general_grants = [
            "--These grants enable Melchi to create objects that track changes.",
            f"GRANT USAGE ON WAREHOUSE {warehouse} TO ROLE {role};",
            f"GRANT USAGE ON DATABASE {self.config["change_tracking_database"]} TO ROLE {role};",
            f"GRANT USAGE, CREATE TABLE, CREATE STREAM ON SCHEMA {self.get_change_tracking_schema_full_name()} TO ROLE {role};",
        ]

        # enable_cdc_statements = ["--These statements enable Melchi to create streams that track changes on the provided tables."]
        database_grants = []
        schema_grants = []
        table_grants = []

        for table in tables:
            database = table['database']
            schema = table['schema']
            table_name = table['table']
            database_grants.append(f"GRANT USAGE ON DATABASE {database} TO ROLE {role};")
            schema_grants.append(f"GRANT USAGE ON SCHEMA {database}.{schema} TO ROLE {role};")
            table_grants.append(f"GRANT SELECT ON TABLE {database}.{schema}.{table_name} TO ROLE {role};")
            # enable_cdc_statements.append(f"ALTER TABLE {database}.{schema}.{table_name} SET CHANGE_TRACKING = TRUE;")

        database_grants = sorted(list(set(database_grants)))
        schema_grants = sorted(list(set(schema_grants)))

        database_grants.insert(0, "--These grants enable Melchi to read changes from your objects.")

        general_grants.append("\n")

        all_grants = create_change_tracking_schema_statement + ["\n"] + general_grants + database_grants + schema_grants + table_grants
        return "\n".join(all_grants)

    def get_data_as_df_for_comparison(self, table_name, order_by_column = None):
        order_by_column = 1 if order_by_column == None else order_by_column
        
        # Get column information
        column_info = self.execute_query(f"DESC TABLE {table_name}", return_results=True)
        
        column_expressions = []
        timestamp_tz_columns = []
        for col in column_info:
            col_name, col_type = col[0], col[1].lower()
            if "timestamp_tz" in col_type or "timestamp_ltz" in col_type:
                column_expressions.append(f"TO_CHAR({col_name}, 'YYYY-MM-DD HH24:MI:SS.FF6TZH:TZM') AS {col_name}")
                timestamp_tz_columns.append(col_name)
            else:
                column_expressions.append(col_name)
        
        query = f"SELECT {', '.join(column_expressions)} FROM {table_name} ORDER BY {order_by_column}"
        df = self.get_data_as_df(query)
        processed_df = TypeMapper.process_df_snowflake_to_duckdb(df)

        # Format timestamp columns
        for col in timestamp_tz_columns:
            processed_df[col] = processed_df[col].apply(lambda x: re.sub(r'([-+]\d{2}):(\d{2})$', r'\1\2', x))
        
        # Convert all columns to string for consistent comparison
        processed_df = processed_df.astype(str)
        
        return processed_df

    def set_timezone(self, tz):
        try:
            self.cursor.execute(f"ALTER SESSION SET TIMEZONE = '{tz}';")
        except Exception as e:
            print(f"Error setting timezone: {str(e)}")
            raise

    def find_problems(self, tables_to_transfer):
        problems = []
        for table_info in tables_to_transfer:
            print(table_info)
            try:
                cdc_type = get_cdc_type(table_info)
            except ValueError as e:
                problems.append(f"{self.get_full_table_name(table_info)} has an invalid cdc_type: {table_info['cdc_type']}.  Valid values are append_only_stream, standard_stream, and full_refresh.")
                continue
            if cdc_type == "STANDARD_STREAM" and self.has_geometry_or_geography_column(self.get_schema(table_info)):
                problems.append(f"{self.get_full_table_name(table_info)} has a geometry or geography column.  Snowflake does not support these in standard streams.  Use append_only_streams or full_refresh for tables with these columns.")
        return problems

    def has_geometry_or_geography_column(self, schema):
        for col in schema:
            if col["type"] in ("GEOMETRY", "GEOGRAPHY"):
                return True
        return False
