# src/data_sync.py

from .warehouses.warehouse_factory import WarehouseFactory
from .utils.table_config import get_tables_to_transfer, get_cdc_type


def sync_data(config):
    print("Starting sync_data")
    source_warehouse = WarehouseFactory.create_warehouse(config.source_type, config.source_config)
    target_warehouse = WarehouseFactory.create_warehouse(config.target_type, config.target_config)

    try:
        source_warehouse.connect()
        target_warehouse.connect()
        print("Connected to source and target warehouses")

        tables_to_transfer = get_tables_to_transfer(config)
        for table_info in tables_to_transfer:
            cdc_type = get_cdc_type(table_info)
            print(f"Processing table: {table_info} as {cdc_type} CDC type")
            sync_table(source_warehouse, target_warehouse, table_info)

    finally:
        source_warehouse.disconnect()
        target_warehouse.disconnect()

def sync_table(source_warehouse, target_warehouse, table_info):
    try:
        target_warehouse.begin_transaction()
        cdc_df = source_warehouse.get_updates(table_info)
        target_warehouse.sync_table(table_info, cdc_df)
        target_warehouse.commit_transaction()

        # if there are any issues with commiting into the target database this won't be executed
        # the CDC data will still exist in the source warehouse and will be able to be grabbed the next time sync_data is run
        source_warehouse.cleanup_source(table_info)
    
    except Exception as e:
        source_warehouse.rollback_transaction()
        target_warehouse.rollback_transaction()
        print(f"Error ingesting data sync: {e}")
        raise

# def fully_refresh_table(source_warehouse, target_warehouse, table_info):
#     try:
#         target_warehouse.begin_transaction()
#         cdc_df = source_warehouse.get_data_as_df(table_info)
#         target_warehouse.sync_table(table_info, cdc_df)
#         target_warehouse.commit_transaction()

#         # if there are any issues with commiting into the target database this won't be executed
#         # the CDC data will still exist in the source warehouse and will be able to be grabbed the next time sync_data is run
#         source_warehouse.cleanup_source(table_info)
#     except Exception as e:
#         source_warehouse.rollback_transaction()
#         target_warehouse.rollback_transaction()
#         print(f"Error ingesting data sync: {e}")
#         raise