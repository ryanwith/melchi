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
        schema = []
        for row in self.cursor.fetchall():
            schema.append(self.format_schema_row(row))
        return schema

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
        if self.replace_existing_tables == True:
            create_query = f"CREATE OR REPLACE TABLE {stream_processing_table} LIKE {table_name};"
        else:
            create_query = f"CREATE TABLE {stream_processing_table} IF NOT EXISTS LIKE {table_name};"
        create_stream_processing_table_queries = [create_query,
            f"ALTER TABLE {stream_processing_table} ADD COLUMN IF NOT EXISTS\"METADATA$ACTION\" varchar;",
            f"ALTER TABLE {stream_processing_table} ADD COLUMN IF NOT EXISTS \"METADATA$ISUPDATE\" varchar;",
            f"ALTER TABLE {stream_processing_table} ADD COLUMN IF NOT EXISTS \"METADATA$ROW_ID\" varchar;"
        ]
        for query in create_stream_processing_table_queries:
            self.cursor.execute(query)

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
    
    def execute_query(self, query_text):
        self.cursor.execute(query_text)

    def fetch_results(self, num = None):
        if num is None:
            results = self.cursor.fetchall()
        elif num == 1:
            results = self.cursor.fetchone()
        elif num > 1:
            results = self.cursor.fetchall()
        else:
            raise ValueError(f"Invalid value for 'num': {num}. Expected None or a positive integer.")
        return results   
    
    def sync_table(self, table_info, df):
        pass

    def convert_cursor_results_to_df(self, results):
        pd.DataFrame(results, columns=[desc[0] for desc in self.cursor.description])

    def replace_existing_tables(self):
        replace_existing = self.config.get("replace_existing", False)
        if replace_existing == True:
            return True
        else:
            return False
        
    def format_schema_row(self, row):
        return {
            "name": row[0],
            "type": row[1],
            "nullable": True if row[3] == "Y" else False,
            "default_value": row[4],
            "primary_key": True if row[3] == "Y" else False
        }