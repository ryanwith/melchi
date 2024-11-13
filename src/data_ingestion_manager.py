# src/data_ingestion_manager.py

from uuid import uuid4
from .utils.table_config import get_cdc_type
from .warehouses.type_mappings import TypeMapper

class DataIngestionManager:
    def __init__(self, source_warehouse, target_warehouse):
        self.source_warehouse = source_warehouse
        self.target_warehouse = target_warehouse

    def sync_table(self, table_info):
        """Manages the end-to-end CDC process for a single table."""
        cdc_type = get_cdc_type(table_info).upper()
        if cdc_type not in self.source_warehouse.get_supported_cdc_types():
            error_message = f"Invalid cdc_type \"{cdc_type}\" selected for {self.source_warehouse.get_full_table_name(table_info)}.  "
            error_message += f"\"{cdc_type}\" is not supported {self.source_warehouse.warehouse_type} databases."
            raise ValueError(error_message)
        
        if cdc_type == "FULL_REFRESH":
            self._handle_full_refresh(table_info)
        elif cdc_type in ("STANDARD_STREAM", "APPEND_ONLY_STREAM"):
            self._handle_stream_updates(table_info)

    def _handle_full_refresh(self, table_info):
        """Handles full refresh CDC type."""

        etl_id = uuid4()
        df_processing_function = TypeMapper.get_df_processing_function(self.source_warehouse, self.target_warehouse)

        try:
            self.target_warehouse.connect()
            self.source_warehouse.connect()
            self.target_warehouse.begin_transaction()
            self.target_warehouse.truncate_table(table_info)
            df_batches = self.source_warehouse.get_batches_for_full_refresh(table_info)
            self.target_warehouse.process_insert_batches(table_info, df_batches, df_processing_function)

            self.target_warehouse.update_cdc_trackers(table_info, etl_id)
            self.target_warehouse.commit_transaction()

        except Exception as e:
            print(f"Error processing full refresh for {self.source_warehouse.get_full_table_name(table_info)}: {e}")
            self.target_warehouse.rollback_transaction()

        finally:
            self.target_warehouse.disconnect()
            self.source_warehouse.disconnect()

    def _handle_stream_updates(self, table_info):

        etl_id = uuid4()
        df_processing_function = TypeMapper.get_df_processing_function(self.source_warehouse, self.target_warehouse)

        try:
            self.target_warehouse.connect()
            self.source_warehouse.connect()
            self.target_warehouse.begin_transaction()
            self.source_warehouse.begin_transaction()
            process_deletes = True if get_cdc_type(table_info) == "STANDARD_STREAM" else False
            completed_transaction_etl_ids = self.target_warehouse.get_etl_ids(table_info)
            self.source_warehouse.prepare_stream_ingestion(table_info, etl_id, completed_transaction_etl_ids)
            # process deletes first so that upserts are handled correctly
            if process_deletes == True:
                df_delete_batches = self.source_warehouse.get_delete_batches_for_stream(table_info)
                self.target_warehouse.process_delete_batches(table_info, df_delete_batches, df_processing_function)
            df_insert_batches = self.source_warehouse.get_insert_batches_for_stream(table_info)

            self.target_warehouse.process_insert_batches(table_info, df_insert_batches, df_processing_function)
            self.target_warehouse.update_cdc_trackers(table_info, etl_id)
            self.target_warehouse.commit_transaction()

            self.source_warehouse.cleanup_source(table_info)
            self.source_warehouse.commit_transaction()

        except Exception as e:
            print(f"Error processing stream updates for {self.source_warehouse.get_full_table_name(table_info)}: {e}")
            self.target_warehouse.rollback_transaction()
            self.source_warehouse.rollback_transaction()

        finally:
            self.source_warehouse.disconnect()
            self.target_warehouse.disconnect()