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

    # SCHEMA AND TABLE MANAGEMENT

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

    # gets a table name in a way that can be queried
    @abstractmethod
    def get_full_table_name(self, table_info):
        pass    

    @abstractmethod
    def replace_existing(self):
        pass

    # CHANGE TRACKING MANAGEMENT

    # out: gets the full name of change_tracking_schema that the Melchi tables are created in for a specific database
    @abstractmethod
    def get_change_tracking_schema_full_name(self):
        pass

    # sets up a warehouse environment as a source or target based on the config
    @abstractmethod
    def setup_environment(self, tables_to_transfer = None):
        pass


    # DATA MOVEMENT

    # input: table_info object
    # dataframe containing records that need to be changed including:
        # melchi_row_id
        # melchi metadata action of insert or delete

    # input: table_info object
    # output: finishes up any actions needed to complete CDC in the source
    @abstractmethod
    def cleanup_source(self, table_info):
        pass

    @abstractmethod
    def get_records_to_insert(self, table_info):
        pass

    @abstractmethod
    def get_records_to_delete(self, table_info):
        pass

    @abstractmethod
    def get_existing_records(self, table_info):
        pass

    @abstractmethod
    def get_cdc_columns(self, table_info):
        """
        Returns the column names used for CDC tracking.
        
        Returns:
            Dict with keys:
                created_at: Column tracking record creation
                updated_at: Column tracking record updates
                deleted_at: Column tracking soft deletes (if applicable)
        """
        pass

    def get_updates(self, table_info, existing_etl_ids, new_etl_id):
        """
        Retrieves CDC data from stream table and returns changes in a dictionary format.
        For standard streams: returns both delete and insert records
        For append-only streams: returns only insert records
        For full refresh: returns only insert records
        """
        cdc_type = get_cdc_type(table_info).upper()

        updates_dict = {
            "records_to_delete": None,
            "records_to_insert": None,
            "records_to_keep": None
        }

        if cdc_type not in self.get_supported_cdc_types():
            raise ValueError(f"{cdc_type} is not a supported CDC type for {self.warehouse_type} sources")
        
        if cdc_type in ("APPEND_ONLY_STREAM", "STANDARD_STREAM"):
            self._prepare_streams(table_info, existing_etl_ids, new_etl_id)

            self._
                # cleans up existing
                # ingests new records into stream


            self._get_new_stream_records(table_info, )

            stream_processing_table_name = self.get_stream_processing_table_name(table_info)
            stream_name = self.get_stream_name(table_info)
            
            # Load stream data into processing table
            self.cursor.execute(f"INSERT INTO {stream_processing_table_name} SELECT *, '{new_etl_id}' FROM {stream_name};")
            self.cursor.execute(f"UPDATE {stream_processing_table_name} SET etl_id = '{new_etl_id}'")

            if cdc_type == "STANDARD_STREAM":
                # For standard streams, get primary keys of records to delete
                primary_keys = self.get_primary_keys(table_info)
                if primary_keys == []:
                    primary_keys = ['METADATA$ROW_ID as MELCHI_ROW_ID']
                pk_columns = ", ".join(primary_keys)
                delete_query = f"""
                    SELECT {pk_columns}
                    FROM {stream_processing_table_name}
                    WHERE "METADATA$ACTION" = 'DELETE'
                """
                updates_dict['records_to_delete'] = self.get_df_batches(delete_query)

            # Get records to insert (for both stream types)
            insert_query = f"""
                SELECT * 
                FROM {stream_processing_table_name}
                WHERE "METADATA$ACTION" = 'INSERT'
            """
            raw_batches = self.get_df_batches(insert_query)
            processed_batches = []

            for batch in raw_batches:
                batch.rename(columns={
                    "METADATA$ROW_ID": "MELCHI_ROW_ID",
                    "METADATA$ACTION": "MELCHI_METADATA_ACTION"
                }, inplace=True)
                processed_batches.append(batch)
            updates_dict['records_to_insert'] = processed_batches

        elif cdc_type == "FULL_REFRESH":
            # For full refresh, we only need insert records
            updates = self.get_df_batches(
                f"SELECT * FROM {self.get_full_table_name(table_info)}"
            )
            all_updates = []
            for df in updates:
                all_updates.append(df)
            updates_dict['records_to_insert'] = all_updates
        return updates_dict

    # Modified sync_table method to use template pattern
    def sync_table(self, table_info, updates_dict: Dict, etl_id: str):
        """Template method that delegates to specific CDC implementations."""
        
        cdc_type = self.get_cdc_type(table_info)
        if cdc_type not in self.get_supported_cdc_types():
            raise ValueError(f"Unsupported CDC type: {cdc_type}")
        if cdc_type == "TIMESTAMP_BASED":
            self._process_timestamp_based_cdc(table_info, etl_id)
        elif cdc_type == "STANDARD_STREAM":
            self._process_standard_stream(table_info, updates_dict, etl_id)
        elif cdc_type == "APPEND_ONLY_STREAM":
            self._process_append_only_stream(table_info, updates_dict, etl_id)
        elif cdc_type == "FULL_REFRESH":
            self._process_full_refresh(table_info, updates_dict, etl_id)
        else:
            raise ValueError(f"Unsupported CDC type: {cdc_type}")

    @abstractmethod
    def get_supported_cdc_types(self, cdc_type):
        pass

    def sync_table_with_source(self, table_info):
        cdc_type = self.get_cdc_type(table_info)
        
        if cdc_type == "TIMESTAMP_BASED":
            self._process_timestamp_based_cdc(table_info)
        elif cdc_type == "STANDARD_STREAM":
            self._process_standard_stream(table_info)
        elif cdc_type == "APPEND_ONLY_STREAM":
            self._process_append_only_stream(table_info)
        elif cdc_type == "FULL_REFRESH":
            self._process_full_refresh(table_info)
        else:
            raise ValueError(f"Unsupported CDC type: {cdc_type}")

    # UTILITY METHODS

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
                column['type'] = mapping_method(column['type'])
            return target_schema
        else:
            raise NotImplementedError(f"Type mapping from {self.warehouse_type} to {target_warehouse_type} is not implemented")               
   
    @abstractmethod
    def generate_source_sql(self):
        pass

    def warehouse_type(self):
        return self.config['type'].upper()
    
    @abstractmethod
    def get_primary_keys(self, table_info):
        pass

    @abstractmethod
    def get_data_as_df_for_comparison(self, table_name, order_by_column = None):
        """
        Retrieves data as a DataFrame, with adjustments for consistent comparison across different warehouse types.
        """
        pass

    @abstractmethod
    def get_df_batches(self, query_text):
        """
        Gets all data from a table as a DataFrame.
        Used for full refresh operations.
        """
        pass
    @abstractmethod
    def set_timezone(self, tz):
        """
        Sets the timezone for the warehouse
        """
        pass

    @abstractmethod
    def get_auth_type(self):
        "Returns the auth type for the warehouse"
        pass
