from abc import ABC, abstractmethod
from .type_mappings import TypeMapper

class AbstractWarehouse(ABC):
    def __init__(self, warehouse_type):
        self.warehouse_type = warehouse_type

    # creates a connection to a data warehouse and preps it for querying
    # sets session variables and creates cursors as required
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def begin_transaction(self):
        pass

    @abstractmethod
    def commit_transaction(self):
        pass

    @abstractmethod
    def rollback_transaction(self):
        pass

    # input: table_info dict including the following strings:
        # table 
        # schema
        # database (optional, included if the warehouse uses databases)
    # output: an array of dictionaries containing column data.  includes the following:
        # name: string
        # type: string
        # nullable: boolean
        # default_value: string
        # primary_key: boolean
    @abstractmethod
    def get_schema(self, table_info):
        pass

    # input: table_info dict, source_schema, target_schema
    # output:
        # creates a table with the target_schema in the target_warehouse
        # updates the table table_info in the target warehouse with metadata
        # updates the table source_columns in the target warehouse with the table's schema int he source warehouse
    @abstractmethod
    def create_table(self, table_info, source_schema, target_schema):
        pass

    # input: table you want to get data from
    # output: returns alls records in the specified table
    @abstractmethod
    def get_data(self, table_name):
        pass

    # input: table you want to get data from
    # output: all records in that table as a data frame
    @abstractmethod
    def get_data_as_df(self, table_name):
        pass
    
    @abstractmethod
    def insert_data(self, table_name, data):
        pass

    # sets up a warehouse environment as a source or target based on the config
    @abstractmethod
    def setup_environment(self, tables_to_transfer = None):
        pass

    @abstractmethod
    def create_cdc_stream(self, table_info):
        pass

    @abstractmethod
    def get_changes(self, table_info):
        pass

    @abstractmethod
    def get_full_table_name(self, table_info):
        pass

    @abstractmethod
    def sync_table(self, table_info, df):
        pass

    @abstractmethod
    def get_stream_name(self, table_info):
        pass

    @abstractmethod
    def cleanup_cdc_for_table(self, table_info):
        pass

    @abstractmethod
    def execute_query(self, query_text):
        pass

    @abstractmethod
    def fetch_results(self, num):
        pass    

    def get_metadata_schema(self):
        return self.config["cdc_metadata_schema"]

    def map_schema_to(self, table_info, target_warehouse_type):
        method_name = f"{self.warehouse_type}_to_{target_warehouse_type}"
        mapping_method = getattr(TypeMapper, method_name, None)
        target_schema = self.get_schema(table_info)
        if mapping_method:
            for column in target_schema:
                column["type"] = mapping_method(column["type"])
            return target_schema
        else:
            raise NotImplementedError(f"Type mapping from {self.warehouse_type} to {target_warehouse_type} is not implemented")               

    def map_type_to(self, target_warehouse_type, source_type):
        method_name = f"{self.warehouse_type}_to_{target_warehouse_type}"
        mapping_method = getattr(TypeMapper, method_name, None)
        if mapping_method:
            return mapping_method(source_type)
        else:
            raise NotImplementedError(f"Type mapping from {self.warehouse_type} to {target_warehouse_type} is not implemented")

    @abstractmethod
    def insert_df(self, table_info, df):
        pass
    
    @abstractmethod
    def replace_existing_tables(self):
        pass