# src/warehouses/duckdb_warehouse.py

import duckdb
import datetime
import pandas as pd
from .abstract_warehouse import AbstractWarehouse
from decimal import Decimal
from ..utils.table_config import get_cdc_type
from pprint import pp
from .type_mappings import TypeMapper

class DuckDBWarehouse(AbstractWarehouse):
    """
    DuckDB warehouse implementation that handles data ingestion and CDC operations.
    Manages connections, schema operations, and change tracking for DuckDB databases.
    """

    def __init__(self, config):
        super().__init__("duckdb")  # Initialize with the warehouse type
        self.config = config
        self.connection = None

    # CONNECTION METHODS

    def connect(self):
        """Creates a connection to a DuckDB database allowing you to query it."""
        self.connection = duckdb.connect(self.config['database'])

        # install and load spatial extension
        # doing this everytime rather than checking it it's installed/needed since there's minimal overhead
        self.connection.execute("INSTALL spatial;")
        self.connection.execute("LOAD spatial;")

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    # TRANSACTION MANAGEMENT

    def begin_transaction(self):
        self.connection.begin()

    def commit_transaction(self):
        self.connection.commit()

    def rollback_transaction(self):
        self.connection.rollback()

    # SCHEMA AND TABLE MANAGEMENT

    def get_schema(self, table_info):
        """Returns array of schema dictionaries as provided in format_schema_row."""
        result = self.connection.execute(f"PRAGMA table_info('{self.get_full_table_name(table_info)}');")
        rows = []
        for row in result.fetchall():
            rows.append(self.format_schema_row(row))
        return rows

    def create_table(self, table_info, source_schema, target_schema):
        """
        Creates table with target_schema and adds melchi_id if no primary keys.
        Updates metadata tables with table info and source columns.
        """
        if self.replace_existing() == False and self.table_exists(table_info) == True:
            return
        primary_keys = []
        cdc_type = get_cdc_type(table_info)


        # create the schema in duckdb if needed
        self.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {table_info["schema"]};")
        # build a list of primary keys

        primary_keys = []
        for column in target_schema:
            if column["primary_key"] == True:
                primary_keys.append(column["name"])
        
        print(f"table_name: {table_info['table']}")
        print(f"cdc_type: {cdc_type}")
        print(f"primary_keys length: {len(primary_keys)}")
        # Will used autogenerated keys from snowflake and bigquery if a primary key isn't set and its a standard stream
        # we do not actually set the primary keys in duckdb as it is too eager for enforcement in transactions
        if len(primary_keys) == 0 and cdc_type == "STANDARD_STREAM":
            pk_name = "MELCHI_ROW_ID"
            target_schema.append({
                "name": f"{pk_name}",
                "type": "VARCHAR",
                "nullable": False,
                "default_value": None,
                "primary_key": True
            })
            primary_keys.append(pk_name)

        primary_key_clause = self.convert_list_to_duckdb_syntax(primary_keys)

        create_table_query = self.generate_create_table_statement(table_info, target_schema)
        # create the actual table
        self.connection.execute(create_table_query)
        current_timestamp = datetime.datetime.now()

        source_columns = []

        for column in source_schema:
            source_db = f"'{table_info["database"]}'"
            source_schema_name = f"'{table_info["schema"]}'"
            source_table = f"'{table_info["table"]}'"
            name = f"'{column["name"]}'"
            type = f"'{column["type"]}'"
            nullable = "TRUE" if column["nullable"] == True else "FALSE"
            default_value = f"'{self.format_value_for_insert(column["default_value"])}'" if column["default_value"] else "NULL"
            primary_key = "TRUE" if column["primary_key"] == True else "FALSE"
            source_column_values = f"""(
                {source_db}, {source_schema_name}, {source_table}, {name}, {type}, {default_value}, {nullable}, {primary_key}
            )"""
            source_columns.append(source_column_values)

        current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Separate the queries into two distinct statements
        update_logs = [
            f"""DELETE FROM {self.get_change_tracking_schema_full_name()}.captured_tables WHERE schema_name = '{table_info["schema"]}' and table_name = '{table_info["table"]}';""",
            f"""DELETE FROM {self.get_change_tracking_schema_full_name()}.source_columns WHERE table_schema = '{table_info["schema"]}' and table_name = '{table_info["table"]}';""",
            f"""INSERT INTO {self.get_change_tracking_schema_full_name()}.captured_tables VALUES ('{table_info["schema"]}', '{table_info["table"]}', '{current_timestamp}', '{current_timestamp}', {primary_key_clause}, '{cdc_type}');""",
            f"""INSERT INTO {self.get_change_tracking_schema_full_name()}.source_columns VALUES {(", ").join(source_columns)};"""
        ]

        for query in update_logs:
            self.connection.execute(query)

    def get_full_table_name(self, table_info):
        """Returns fully qualified table name."""
        return f"{table_info["schema"]}.{table_info["table"]}"

    def replace_existing(self):
        """Returns whether existing tables should be replaced."""
        return self.config["replace_existing"]

    def format_schema_row(self, row):
        """Formats a column for a schema."""
        return {
            "name": row[1],
            "type": row[2],
            "nullable": True if row[3] == "TRUE".upper() else False,
            "default_value": row[4],
            "primary_key": True if row[5] == "TRUE".upper() else False,
        }

    def generate_create_table_statement(self, table_info, schema):
        """Generates SQL statement for table creation."""
        if self.replace_existing() == True:
            create_statement = f"CREATE OR REPLACE TABLE {self.get_full_table_name(table_info)} "
        else:
            create_statement = f"CREATE TABLE IF NOT EXISTS {self.get_full_table_name(table_info)} "
        column_statements = []
        for col in schema:
            column_statement = f"{col["name"]} {col["type"]}"
            column_statement += " NOT NULL" if col["nullable"] == False else ""
            column_statements.append(column_statement)
        full_create_statement = f"{create_statement}({", ".join(column_statements)});"

        return full_create_statement

    def contains_spatial(self, schema):
        """Checks if schema contains spatial data types."""
        for column in schema:
            if column["type"] == "GEOMETRY":
                return True
        return False

    # CHANGE TRACKING MANAGEMENT

    def get_change_tracking_schema_full_name(self):
        """Returns the change tracking schema name."""
        return self.config["change_tracking_schema"]

    def setup_environment(self):
        """Sets up environment based on warehouse role."""
        if self.config["warehouse_role"] == "TARGET":
            self.setup_target_environment()
        elif self.config["warehouse_role"] == "SOURCE":
            raise NotImplementedError(f"DuckDB is not yet supported as a source")
        else:
            raise ValueError(f"Unknown warehouse role: {self.config["warehoues_role"]}")

    def setup_target_environment(self):
        """Creates metadata tables for CDC tracking and source schema information."""
        self.connection.execute(f"CREATE SCHEMA IF NOT EXISTS {self.get_change_tracking_schema_full_name()};")
        if self.replace_existing() == True:
            beginning_of_query = "CREATE OR REPLACE TABLE"
        else:
            beginning_of_query = "CREATE TABLE IF NOT EXISTS"
        self.connection.execute(f"""{beginning_of_query} {self.get_change_tracking_schema_full_name()}"""
                                + """.captured_tables (schema_name varchar, table_name varchar, created_at timestamp, updated_at timestamp, primary_keys varchar[], cdc_type varchar);""")
        self.connection.execute(f"""{beginning_of_query} {self.get_change_tracking_schema_full_name()}"""
                                + """.source_columns (table_catalog varchar, table_schema varchar, table_name varchar, column_name varchar, data_type varchar, column_default varchar, is_nullable boolean, primary_key boolean);""")

    # DATA SYNCHRONIZATION

    def sync_table(self, table_info, updates_dict):
        """
        Syncs changes from source to target table, processing batches of deletes and inserts.
        Updates come as a dictionary with optional records_to_delete and records_to_insert DataFrames.
        """
        full_table_name = self.get_full_table_name(table_info)
        cdc_type = get_cdc_type(table_info)
        insert_batches = updates_dict.get("records_to_insert")

        data_inserted = False
        if cdc_type == "FULL_REFRESH":
            if insert_batches is not None:
                self.connection.execute(f"TRUNCATE TABLE {full_table_name};")
                data_inserted = self._process_insert_batches(insert_batches, table_info)
        elif cdc_type in ("APPEND_ONLY_STREAM", "STANDARD_STREAM"):
            if cdc_type == "STANDARD_STREAM" and updates_dict.get("records_to_delete") is not None:
                # deletes must be processed first
                delete_batches = updates_dict["records_to_delete"]
                self._process_delete_batches(delete_batches, table_info)
            if insert_batches is not None:
                # process inserts after
                data_inserted = self._process_insert_batches(insert_batches, table_info)  
        else: #this condition should never be hit as get_cdc_type validates the cdc_type
            raise ValueError(f"{cdc_type} is not a valid CDC type.  Please provide FULL_REFRESH, STANDARD_STREAM, or APPEND_ONLY_STREAM, or leave it blank to default to FULL_REFRESH.")
        if data_inserted == False:
            print(f"No records ingested into {full_table_name}.")

        self.update_cdc_tracker(table_info)
        # update info of last update time

    def _process_delete_batches(self, deletes_df, table_info):
        """Process deletes in batches using primary keys."""
        full_table_name = self.get_full_table_name(table_info)
        temp_table = f"{table_info["table"]}_deletes_temp"

        primary_keys = None
        formatted_primary_keys = None
        iterations = 0
        try:
            for batch in deletes_df:

                try:
                    if iterations == 0:
                        primary_keys = self.get_primary_keys(table_info)
                        formatted_primary_keys = ", ".join(primary_keys)
                    iterations += 1
                    self.connection.execute(f"CREATE OR REPLACE TEMP TABLE {temp_table} AS (SELECT * FROM batch);")
                    
                    delete_sql = f"""
                        DELETE FROM {full_table_name}
                        WHERE ({formatted_primary_keys}) IN (
                            SELECT ({formatted_primary_keys}) FROM {temp_table}
                        );
                    """
                    self.connection.execute(delete_sql)
                except Exception as e:
                    print(f"Error processing delete batch for {full_table_name}: {str(e)}")
                    # Later: Add error status to captured_tables
                    continue
        finally:
            self.connection.execute(f"DROP TABLE IF EXISTS {temp_table};") if iterations > 0 else None

    def _process_insert_batches(self, df, table_info):
        """Process batches of inserts directly from DataFrame."""
        full_table_name = self.get_full_table_name(table_info)
        formatted_columns = None
        iterations = 0
        batches_exist = False
        for batch in df:
            if iterations == 0:
                formatted_columns = ", ".join([x["name"] for x in self.get_schema(table_info)])
            iterations += 1
            batches_exist = True
            try:
                processed_batch = TypeMapper.process_df_snowflake_to_duckdb(batch)
                self.connection.execute(f"INSERT INTO {full_table_name} (SELECT {formatted_columns} FROM processed_batch);")
            except Exception as e:
                print(f"Error processing insert batch for {full_table_name}: {str(e)}")
                # Later: Add error status to captured_tables
                continue
        return batches_exist    
        
    def cleanup_source(self, table_info):
        pass

    def update_cdc_tracker(self, table_info):
        """Updates the captured_tables table with the time the last CDC operation ran."""
        where_clause = f"WHERE table_name = '{table_info["table"]}' and schema_name = '{table_info["schema"]}'"
        self.connection.execute(f"UPDATE {self.get_change_tracking_schema_full_name()}.captured_tables SET updated_at = current_timestamp {where_clause};")

    def get_primary_keys(self, table_info):
        """Gets the primary keys for a specific table."""
        captured_tables = f"{self.get_change_tracking_schema_full_name()}.captured_tables"
        get_primary_keys_query = f"""SELECT primary_keys FROM {captured_tables}
                WHERE table_name = '{table_info["table"]}' and schema_name = '{table_info["schema"]}'"""
        primary_keys = sorted(self.connection.execute(get_primary_keys_query).fetchone()[0])
        return primary_keys
    
    def get_df_batches(self, query_text):
        try:
            # First, get the result set
            result = self.connection.execute(query_text)
            
            # Get column names and types
            column_info = result.description
            
            # Prepare a list to store modified column expressions
            modified_columns = []
            
            for col in column_info:
                col_name = col[0]
                col_type = col[1]
                
                # Convert only DATE and TIME types to varchar
                if col_type.upper() in ['DATE', 'TIME']:
                    modified_columns.append(f"CAST({col_name} AS VARCHAR) AS {col_name}")
                else:
                    modified_columns.append(col_name)
            
            # Construct a new query with type casts
            modified_query = f"SELECT {', '.join(modified_columns)} FROM ({query_text.strip(";")}) subquery"
            # Execute the modified query and return as DataFrame
            df = self.connection.execute(modified_query).df()
            return df
        except Exception as e:
            print(f"Error executing query: {query_text}")
            print(f"Error details: {str(e)}")
            raise

    def get_data_as_df_for_comparison(self, table_name, order_by_column = None):
        order_by_column = 1 if order_by_column == None else order_by_column
        
        column_types_query = f"PRAGMA table_info('{table_name}')"
        column_types_raw = self.execute_query(column_types_query, True)

        column_expressions = []
        binary_columns = []
        geometry_columns = []
        for col in column_types_raw:
            col_name, col_type = col[1], col[2].lower()
            if 'decimal' in col_type or 'numeric' in col_type:
                column_expressions.append(f"CAST({col_name} AS VARCHAR) AS {col_name}")
            elif "binary" in col_name.lower():
                column_expressions.append(col_name)
                binary_columns.append(col_name)
            elif 'timestamp with time zone' in col_type:
                # Format timestamp columns consistently with 6 decimal places for fractional seconds
                # column_expressions.append(f"STRFTIME({col_name}, '%Y-%m-%d %H:%M:%S.%f%z') AS {col_name}")
                column_expressions.append(f"""
                    CASE 
                        WHEN LENGTH(STRFTIME({col_name}, '%z')) = 3 
                        THEN STRFTIME({col_name}, '%Y-%m-%d %H:%M:%S.%f') || STRFTIME({col_name}, '%z') || '00'
                        ELSE STRFTIME({col_name}, '%Y-%m-%d %H:%M:%S.%f%z')
                    END AS {col_name}
                """.strip())

            elif 'date' in col_type:
                column_expressions.append(f"CAST({col_name} AS VARCHAR) AS {col_name}")
            elif 'time' in col_type:
                column_expressions.append(f"CAST({col_name} AS VARCHAR) AS {col_name}")
            elif 'geometry' in col_type:
                column_expressions.append(f"ST_AsText({col_name}) AS {col_name}")
                geometry_columns.append(col_name)
            else:
                column_expressions.append(col_name)

        subquery = f"SELECT {', '.join(column_expressions)} FROM {table_name} order by {order_by_column};"
        df = self.get_df_batches(subquery)

        # Convert specific columns to ensure consistent representation
        for col in df.columns:
            if df[col].dtype.name.startswith('decimal'):
                df[col] = df[col].astype(str)
            elif df[col].dtype.name in ['datetime64[ns]', 'date', 'time']:
                df[col] = df[col].astype(str)
            elif 'timestamp' in col.lower():
                # Ensure consistent formatting for timestamp columns
                df[col] = df[col].apply(lambda x: x[:26] + x[26:].replace(':', ''))
            elif col in geometry_columns:
                df[col] = df[col].apply(lambda x: 'POINT(' + x.split('(')[1] if isinstance(x, str) else x)
                df[col] = df[col].apply(lambda x: 'POINT(' + x.split('(')[1] if isinstance(x, str) else x)
                df[col] = df[col].apply(self.normalize_wkt_spacing)

        df = df.astype(str)

        for col in binary_columns:
            df[col] = df[col].apply(lambda x: x[10:-1].replace("\\'", "'") if isinstance(x, str) and len(x) > 11 else x)

        return df

    # UTILITY METHODS

    def execute_query(self, query_text, return_results = False):
        """Executes a query, mainly used for testing."""
        self.connection.execute(query_text)
        if return_results == True:
            return self.connection.fetchall()

    def convert_list_to_duckdb_syntax(self, python_list):
        """Converts Python list to DuckDB array syntax."""
        return f"[{", ".join(list(map(lambda item: f"'{item}'", python_list)))}]"
    
    def format_value_for_insert(self, value):
        if type(value) == str:
            return value.replace("'", "''")
        else:
            return value
        
    def generate_source_sql(self):
        pass

    def format_binary_for_comparison(self, value):
        return value.hex()
    
    def set_timezone(self, tz):
        self.connection.execute(f"SET TIMEZONE = '{tz}';")

    def normalize_wkt_spacing(self, wkt_str):
        if not isinstance(wkt_str, str):
                raise ValueError(f"Expected string WKT geometry, got {type(wkt_str)}: {wkt_str}")

        # Split by first parenthesis to get the geometry type
        parts = wkt_str.split('(', 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid WKT format - missing parentheses: {wkt_str}")
        geom_type = parts[0].strip()
        coordinates = parts[1]
        return f"{geom_type}({coordinates}"
    
    def table_exists(self, table_info):
        table = table_info["table"]
        schema = table_info["schema"]
        query = f"SELECT * FROM information_schema.tables WHERE table_schema = '{schema}' AND table_name = '{table}'"
        results = self.connection.execute(query).fetchone()
        return True if results else False