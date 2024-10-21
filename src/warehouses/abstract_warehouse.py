# src/warehouses/abstract_warehouse.py

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

    # out: gets the full name of change_tracking_schema that the Melchi tables are created in for a specific database
    @abstractmethod
    def get_change_tracking_schema_full_name(self):
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
            # adds a melchi_id column if there are no primary keys specified in teh table
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

    # sets up a warehouse environment as a source or target based on the config
    @abstractmethod
    def setup_environment(self, tables_to_transfer = None):
        pass

    # gets a table name in a way that can be queried
    @abstractmethod
    def get_full_table_name(self, table_info):
        pass

    # input: table_info dictionary
    # output: creates CDC streams used to get changes.  can be multiple tables
    @abstractmethod
    def create_cdc_stream(self, table_info):
        pass

    # input: name of the permanent table you're copying
    # output: the stream name for that permanent table
    @abstractmethod
    def get_stream_name(self, table_info):
        pass

    @abstractmethod
    def sync_table(self, table_info, df):
        pass

    # input: table_info object
    # output: finishes up any actions needed to complete CDC in the source
    @abstractmethod
    def cleanup_source(self, table_info):
        pass

    # input: query to execute
    # output: executes the submitted query, optionally returns results
    # currently just used for testing
    @abstractmethod
    def execute_query(self, query_text, return_results = False):
        pass

    # input: table_info object, the warehouse type it needs to be replicated to
    # output: a schema object (see get schema) with the required types for the new database
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

    
    @abstractmethod
    def replace_existing_tables(self):
        pass