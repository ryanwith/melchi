# src/warehouses/abstract_warehouse.py

from abc import ABC, abstractmethod
from .type_mappings import TypeMapper
from ..utils.table_config import get_cdc_type

class AbstractWarehouse(ABC):

    def __init__(self, warehouse_type):
        self.warehouse_type = warehouse_type

    # CONNECTION METHODS

    # creates a connection to a data warehouse and preps it for querying
    # sets session variables and creates cursors as required
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    # TRANSACTION MANAGEMENT
    @abstractmethod
    def begin_transaction(self):
        pass

    @abstractmethod
    def commit_transaction(self):
        pass

    @abstractmethod
    def rollback_transaction(self):
        pass

    # SETUP ENVIRONMENT METHODS (DDLs)

    # sets up a warehouse environment as a source or target based on the config
    @abstractmethod
    def setup_environment(self, tables_to_transfer = None):
        pass

    @abstractmethod
    def create_table(self, table_info, source_schema, target_schema):
        pass

    # SYNC DATA METHODS

    @abstractmethod
    def prepare_stream_ingestion(self, table_info, etl_id):
        pass

    @abstractmethod
    def truncate_table(self, table_info):
        pass

    @abstractmethod
    def get_df_batches(self, query_text):
        """
        Gets all data from a table as a DataFrame.
        Used for full refresh operations.
        """
        pass

    @abstractmethod
    def process_insert_batches(self, table_info, raw_df_batches, df_processing_function):
        pass

    @abstractmethod
    def process_delete_batches(self, table_info, raw_df_batches, df_processing_function):
        pass

    @abstractmethod
    def get_batches_for_full_refresh(self, table_info):
        pass

    # input: table_info object
    # output: finishes up any actions needed to complete CDC in the source
    @abstractmethod
    def cleanup_source(self, table_info):
        pass

    @abstractmethod
    def update_cdc_trackers(table_info, etl_id):
        pass

    # UTILITY METHODS

    @abstractmethod
    def get_schema(self, table_info):
        pass

    # gets a table name in a way that can be queried
    @abstractmethod
    def get_full_table_name(self, table_info):
        pass    

    @abstractmethod
    def replace_existing(self):
        pass

    # out: gets the full name of change_tracking_schema that the Melchi tables are created in for a specific database
    @abstractmethod
    def get_change_tracking_schema_full_name(self):
        pass

    @abstractmethod
    def generate_source_sql(self):
        pass
    
    @abstractmethod
    def get_primary_keys(self, table_info):
        pass

    @abstractmethod
    def get_supported_cdc_types(self, cdc_type):
        pass

    @abstractmethod
    def get_auth_type(self):
        "Returns the auth type for the warehouse"
        pass

    @abstractmethod
    def execute_query(self, query_text, return_results = False):
        pass

    @abstractmethod
    def get_data_as_df_for_comparison(self, table_name, order_by_column = None):
        """
        Retrieves data as a DataFrame, with adjustments for consistent comparison across different warehouse types.
        """
        pass

    @abstractmethod
    def set_timezone(self, tz):
        """
        Sets the timezone for the warehouse
        """
        pass

    # input: table_info object, the warehouse type it needs to be replicated to
    # output: a schema object (see get schema) with the required types for the new database
    def map_schema_to(self, table_info, target_warehouse_type):
        method_name = f"{self.warehouse_type}_to_{target_warehouse_type}"
        mapping_method = getattr(TypeMapper, method_name, None)
        target_schema = self.get_schema(table_info)
        if mapping_method:
            for column in target_schema:
                column['type'] = mapping_method(column['type'])
            return target_schema
        else:
            raise NotImplementedError(f"Type mapping from {self.warehouse_type} to {target_warehouse_type} is not implemented")               
   