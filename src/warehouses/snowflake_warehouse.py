import snowflake.connector
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

    def get_schema(self, full_table_name):
        self.cursor.execute(f"DESC TABLE {full_table_name}")
        return [(col[0], col[1], col[5]) for col in self.cursor.fetchall()]

    def create_table(self, schema_name, table_name, schema):
        # Implementation for creating a table in Snowflake
        pass

    def get_data(self, table_name):
        self.cursor.execute(f"SELECT * FROM {table_name}")
        return self.cursor.fetchall()

    def insert_data(self, table_name, data):
        # Implementation for inserting data into Snowflake
        pass

    def setup_target_environment(self):
        pass

    def create_cdc_stream(self, table_info):
        database = table_info["database"]
        schema = table_info["schema"]
        table = table_info["table"]
        stream_name = f"{self.config["cdc_schema"]}.{database}${schema}${table}"
        table_name = f"{database}.{schema}.{table}"
        create_stream_query = f"CREATE STREAM IF NOT EXISTS {stream_name} ON TABLE {table_name} SHOW_INITIAL_ROWS = true"
        self.cursor.execute(create_stream_query)      

