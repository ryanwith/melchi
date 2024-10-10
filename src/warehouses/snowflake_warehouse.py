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