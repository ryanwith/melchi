# src/data_sync.py

from .warehouses.warehouse_factory import WarehouseFactory
from .utils.table_config import get_tables_to_transfer


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
            print(f"Processing table: {table_info}")
            # if source_warehouse.config["cdc_strategy"] == "cdc_streams":
            try:
                target_warehouse.begin_transaction()
                cdc_df = source_warehouse.get_cdc_data(table_info)
                print("Sample data before DuckDB insert:", cdc_df['NUMBER_TEST_COL'].head().tolist())
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

    finally:
        source_warehouse.disconnect()
        target_warehouse.disconnect()
