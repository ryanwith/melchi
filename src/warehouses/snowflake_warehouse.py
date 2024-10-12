import snowflake.connector
import pandas as pd
from .abstract_warehouse import AbstractWarehouse

class SnowflakeWarehouse(AbstractWarehouse):
    def __init__(self, config):
        super().__init__("snowflake")  # Initialize with the warehouse type
        self.config = config
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = snowflake.connector.connect(**self.config)
        self.cursor = self.connection.cursor()
        self.cursor.execute(f"USE ROLE {self.config["role"]};")
        self.cursor.execute(f"USE WAREHOUSE {self.config["warehouse"]};")

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def begin_transaction(self):
        self.cursor.execute("BEGIN")

    def commit_transaction(self):
        self.connection.commit()

    def rollback_transaction(self):
        self.connection.rollback()

    def get_schema(self, table_info):
        self.cursor.execute(f"DESC TABLE {self.get_full_table_name(table_info)}")
        return [(col[0], col[1], col[5]) for col in self.cursor.fetchall()]

    def create_table(self, table_info, schema):
        # Implementation for creating a table in Snowflake
        pass

    def get_data(self, table_name):
        self.cursor.execute(f"SELECT * FROM {table_name}")
        return self.cursor.fetchall()
    
    def get_data_as_df(self, table_name):
        results = self.get_data(table_name)
        return pd.DataFrame(results, columns=[desc[0] for desc in self.cursor.description])
        
    
    def get_cdc_data(self, table_info):
        stream_processing_table_name = self.get_stream_processing_table_name(table_info)
        stream_name = self.get_stream_name(table_info)
        changes = self.get_data_as_df(stream_processing_table_name)
        if changes.empty:
            print(f"INSERT INTO {stream_processing_table_name} SELECT * FROM {stream_name};")
            self.cursor.execute(f"INSERT INTO {stream_processing_table_name} SELECT * FROM {stream_name};")
            changes = self.get_data_as_df(stream_processing_table_name)
        changes.rename(columns={"METADATA$ROW_ID": "MELCHI_ROW_ID", "METADATA$ACTION": "MELCHI_METADATA_ACTION"}, inplace=True)
        return changes

    def cleanup_cdc_for_table(self, table_info):
        stream_processing_table_name = self.get_stream_processing_table_name(table_info)
        self.cursor.execute(f"TRUNCATE TABLE {stream_processing_table_name}")

    def insert_data(self, table_name, data):
        # Implementation for inserting data into Snowflake
        pass

    def setup_target_environment(self):
        pass

    def get_changes(self, table_info):
        pass

    def create_cdc_stream(self, table_info):
        stream_name = self.get_stream_name(table_info)
        table_name = f"{table_info["database"]}.{table_info["schema"]}.{table_info["table"]}"
        create_stream_query = f"CREATE STREAM IF NOT EXISTS {stream_name} ON TABLE {table_name} SHOW_INITIAL_ROWS = true"
        self.cursor.execute(create_stream_query)
        stream_processing_table = f"{stream_name}_processing"
        create_stream_processing_table_queries = [f"CREATE TABLE IF NOT EXISTS {stream_processing_table} LIKE {table_name};",
            f"ALTER TABLE {stream_processing_table} ADD COLUMN METADATA$ACTION varchar;",
            f"ALTER TABLE {stream_processing_table} ADD COLUMN \"METADATA$ISUPDATE\" varchar;",
            f"ALTER TABLE {stream_processing_table} ADD COLUMN \"METADATA$ROW_ID\" varchar;"
        ]
        for query in create_stream_processing_table_queries:
            self.cursor.execute(query)
        # # Used to advance streams
        # cdc_cdc_tracker_query =f"""CREATE TABLE IF NOT EXISTS {self.config["cdc_schema"]}.melchi_cdc_tracker 
        #     (database_name varchar, schema_name varchar, table_name varchar, last_cdc timestamp_ltz);"""
        # self.cursor.execute(cdc_cdc_tracker_query)

    def get_stream_name(self, table_info):
        database = table_info["database"]
        schema = table_info["schema"]
        table = table_info["table"]
        return f"{self.config["cdc_schema"]}.{database}${schema}${table}"
    
    def get_stream_processing_table_name(self, table_info):
        database = table_info["database"]
        schema = table_info["schema"]
        table = table_info["table"]
        return f"{self.config["cdc_schema"]}.{database}${schema}${table}_processing"
    
    def get_full_table_name(self, table_info):
        database = table_info["database"]
        schema = table_info["schema"]
        table = table_info["table"]
        return f"{database}.{schema}.{table}"
    
    def sync_table(self, table_info, df):
        pass

    def convert_cursor_results_to_df(self, results):
        pd.DataFrame(results, columns=[desc[0] for desc in self.cursor.description])