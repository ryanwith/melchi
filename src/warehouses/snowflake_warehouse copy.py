# # src/warehouses/snowflake_warehouse.py

# import snowflake.connector
# import pandas as pd
# from .abstract_warehouse import AbstractWarehouse


# class SnowflakeWarehouse(AbstractWarehouse):
#     """
#     Snowflake warehouse implementation that handles data extraction and CDC operations.
#     Manages connections, schema operations, and change tracking for Snowflake databases.
#     """

#     def __init__(self, config):
#         super().__init__("snowflake")  # Initialize with the warehouse type
#         self.config = config
#         self.connection = None
#         self.cursor = None

#     # CONNECTION METHODS
    
#     def connect(self, role = None):
#         if role == None:
#             role = self.config["role"]
#         """Creates a cursor and sets the role and warehouse for operations."""
#         connect_params = {
#             'account': self.config['account'],
#             'user': self.config['user'],
#             'password': self.config['password']
#         }
#         self.connection = snowflake.connector.connect(**connect_params)
#         self.cursor = self.connection.cursor()
#         self.cursor.execute(f"USE ROLE {role};")
#         self.cursor.execute(f"USE WAREHOUSE {self.config["warehouse"]};")

#     def disconnect(self):
#         if self.cursor:
#             self.cursor.close()
#             self.cursor = None
#         if self.connection:
#             self.connection.close()
#             self.connection = None

#     # TRANSACTION MANAGEMENT
    
#     def begin_transaction(self):
#         self.cursor.execute("BEGIN;")

#     def commit_transaction(self):
#         self.connection.commit()

#     def rollback_transaction(self):
#         self.connection.rollback()

#     # SCHEMA AND TABLE MANAGEMENT
    
#     def get_schema(self, table_info):
#         # input: table_info dictionary containing keys database, schema, and table
#         # output: array of schema dictionaries as provided in format_schema_row 
#         if self.connection == None:
#             raise ConnectionError("You have not established a connection to the database")
#         elif self.cursor == None:
#             raise ConnectionError("You do not have a valid cursor")
        
#         self.cursor.execute(f"DESC TABLE {self.get_full_table_name(table_info)}")
#         schema = []
#         for row in self.cursor.fetchall():
#             schema.append(self.format_schema_row(row))
#         return schema

#     def create_table(self, table_info, source_schema, target_schema, cdc_type):
#         # Implementation for creating a table in Snowflake
#         pass

#     def get_full_table_name(self, table_info):
#         database = table_info["database"]
#         schema = table_info["schema"]
#         table = table_info["table"]
#         return f"{database}.{schema}.{table}"
    
#     def replace_existing_tables(self):
#         replace_existing = self.config.get("replace_existing", False)
#         if replace_existing == True:
#             return True
#         else:
#             return False

#     def format_schema_row(self, row):
#         # input: row of the schema as provided in a cursor
#         return {
#             "name": row[0],
#             "type": row[1],
#             "nullable": True if row[3] == "Y" else False,
#             "default_value": row[4],
#             "primary_key": True if row[5] == "Y" else False
#         }

#     # CHANGE TRACKING MANAGEMENT
    
#     def get_change_tracking_schema_full_name(self):
#         return f"{self.config["change_tracking_database"]}.{self.config["change_tracking_schema"]}"
    
#     def setup_environment(self, tables_to_transfer = None):
#         if self.config["warehouse_role"] == "TARGET":
#             raise NotImplementedError(f"Snowflake is not yet supported as a target environment")
#         elif self.config["warehouse_role"] == "SOURCE":
#             self.setup_source_environment(tables_to_transfer)
#         else:
#             raise ValueError(f"Unknown warehouse role: {self.config["warehoues_role"]}")

#     def setup_source_environment(self, tables_to_transfer):
#         """Creates streams and CDC tables for each table to be transferred."""
#         if tables_to_transfer == []:
#             raise Exception("No tables to transfer found")

#         if self.config["cdc_strategy"] == "cdc_streams":
#             for table_info in tables_to_transfer:
#                 self.create_cdc_objects(table_info)
#         else:
#             raise ValueError(f"Invalid or no cdc_strategy provided")

#     def setup_target_environment(self):
#         pass

#     def get_stream_name(self, table_info):
#         """Returns the stream name for the given table."""
#         database = table_info["database"]
#         schema = table_info["schema"]
#         table = table_info["table"]
#         return f"{self.get_change_tracking_schema_full_name()}.{database}${schema}${table}"
    
#     def get_stream_processing_table_name(self, table_info):
#         """Returns the processing table name for the given table's stream."""
#         database = table_info["database"]
#         schema = table_info["schema"]
#         table = table_info["table"]
#         return f"{self.get_change_tracking_schema_full_name()}.{database}${schema}${table}_processing"

#     def create_cdc_objects(self, table_info):
#         """
#         Creates a snowflake stream and permanent table for CDC tracking.
#         Generates unique names for stream and processing table.
#         """
#         stream_name = self.get_stream_name(table_info)
#         table_name = f"{table_info["database"]}.{table_info["schema"]}.{table_info["table"]}"
#         stream_type = table_info["cdc_type"]
#         append_only_statement = f"APPEND_ONLY = {"TRUE" if stream_type == "APPEND_ONLY_STREAM" else "FALSE"}"
#         stream_processing_table = f"{stream_name}_processing"
#         if self.replace_existing_tables() == True:
#             create_query = f"CREATE OR REPLACE TABLE {stream_processing_table} LIKE {table_name};"
#             create_stream_query = f"CREATE OR REPLACE STREAM {stream_name} ON TABLE {table_name} SHOW_INITIAL_ROWS = true {append_only_statement}"
#         else:
#             create_query = f"CREATE TABLE {stream_processing_table} IF NOT EXISTS LIKE {table_name};"
#             create_stream_query = f"CREATE STREAM IF NOT EXISTS {stream_name} ON TABLE {table_name} SHOW_INITIAL_ROWS = true {append_only_statement}"
#         create_stream_processing_table_queries = [
#             create_stream_query,
#             create_query,
#             f"ALTER TABLE {stream_processing_table} ADD COLUMN IF NOT EXISTS\"METADATA$ACTION\" varchar;",
#             f"ALTER TABLE {stream_processing_table} ADD COLUMN IF NOT EXISTS \"METADATA$ISUPDATE\" varchar;",
#             f"ALTER TABLE {stream_processing_table} ADD COLUMN IF NOT EXISTS \"METADATA$ROW_ID\" varchar;"
#         ]
#         for query in create_stream_processing_table_queries:
#             self.cursor.execute(query)


#     # DATA SYNCHRONIZATION
    
#     def sync_table(self, table_info, df):
#         raise NotImplementedError("Snowflake is not yet supported as a target")

#     def cleanup_source(self, table_info):
#         """Removes processed records from the stream processing table."""
#         stream_processing_table_name = self.get_stream_processing_table_name(table_info)
#         self.cursor.execute(f"TRUNCATE TABLE {stream_processing_table_name}")    

#     def get_cdc_data(self, table_info):
#         """
#         Retrieves CDC data from stream table and returns changes.
#         Advances the stream offset and returns changes as a DataFrame.
#         """
#         stream_processing_table_name = self.get_stream_processing_table_name(table_info)
#         stream_name = self.get_stream_name(table_info)
#         self.cursor.execute(f"INSERT INTO {stream_processing_table_name} SELECT * FROM {stream_name};")
#         changes = self.get_data_as_df(f"SELECT * FROM {stream_processing_table_name}")
#         changes.rename(columns={"METADATA$ROW_ID": "MELCHI_ROW_ID", "METADATA$ACTION": "MELCHI_METADATA_ACTION"}, inplace=True)
#         return changes

#     # UTILITY METHODS
    
#     def execute_query(self, query_text, return_results = False):
#         """Executes a query and optionally returns results."""
#         self.cursor.execute(query_text)
#         if return_results:
#             return self.cursor.fetchall()

#     def get_data_as_df(self, query_text):
#         print(f"Executing query: {query_text}")
#         try:
#             cursor = self.connection.cursor()
#             cursor.execute(query_text)
#             print("Query executed successfully")
#             try:
#                 df = cursor.fetch_pandas_all()
#                 print(f"Data fetched as DataFrame. Shape: {df.shape}")
#                 return df
#             except Exception as e:
#                 print(f"Error fetching as DataFrame: {str(e)}")
#                 raise
#         except Exception as e:
#             print(f"Error executing query: {str(e)}")
#             raise
    
#     def get_primary_keys(self, table_info):
#         schema = self.get_schema(table_info)
#         return sorted([col['name'] for col in schema if col['primary_key']])

#     def generate_source_sql(self, tables):
#         role = self.config['role']
#         warehouse = self.config['warehouse']

#         header_comments = [
#             "-- IMPORTANT: These statements must be executed with appropriate roles that have:",
#             "-- * Permission to create/modify databases and schemas",
#             "-- * Permission to grant privileges",
#             "-- * Permission to modify objects in the target schemas",
#             "-- Multiple roles may be needed depending on your security model.",
#             "\n"
#         ]

#         create_change_tracking_schema_statement = [
#             "--This command creates the change tracking schema.  Not required if it already exists.",
#             f"CREATE SCHEMA IF NOT EXISTS {self.get_change_tracking_schema_full_name()};",
#             "\n"
#         ]

#         general_grants = [
#             "--These grants enable Melchi to create objects that track changes.",
#             f"GRANT USAGE ON WAREHOUSE {warehouse} TO ROLE {role};",
#             f"GRANT USAGE ON DATABASE {self.config["change_tracking_database"]} TO ROLE {role};",
#             f"GRANT USAGE, CREATE TABLE, CREATE STREAM ON SCHEMA {self.get_change_tracking_schema_full_name()} TO ROLE {role};",
#         ]

#         # enable_cdc_statements = ["--These statements enable Melchi to create streams that track changes on the provided tables."]
#         database_grants = []
#         schema_grants = []
#         table_grants = []

#         for table in tables:
#             database = table['database']
#             schema = table['schema']
#             table_name = table['table']
#             database_grants.append(f"GRANT USAGE ON DATABASE {database} TO ROLE {role};")
#             schema_grants.append(f"GRANT USAGE ON SCHEMA {database}.{schema} TO ROLE {role};")
#             table_grants.append(f"GRANT SELECT ON TABLE {database}.{schema}.{table_name} TO ROLE {role};")
#             # enable_cdc_statements.append(f"ALTER TABLE {database}.{schema}.{table_name} SET CHANGE_TRACKING = TRUE;")

#         database_grants = sorted(list(set(database_grants)))
#         schema_grants = sorted(list(set(schema_grants)))

#         database_grants.insert(0, "--These grants enable Melchi to read changes from your objects.")

#         general_grants.append("\n")

#         all_grants = create_change_tracking_schema_statement + ["\n"] + general_grants + database_grants + schema_grants + table_grants
#         return "\n".join(all_grants)

#     def get_data_as_df_for_comparison(self, table_name):
#         # self.execute_query("ALTER SESSION SET NUMBER_FORMAT = 'E30;")
#         return self.get_data_as_df(f"SELECT * FROM {table_name};")
