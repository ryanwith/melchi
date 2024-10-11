from .warehouses.warehouse_factory import WarehouseFactory
from .utils.table_config import get_tables_to_transfer

def setup_source(config, tables = None):
    source_warehouse = WarehouseFactory.create_warehouse(config.source_type, config.source_config)
    target_warehouse = WarehouseFactory.create_warehouse(config.target_type, config.target_config)

    try:
        source_warehouse.connect()
        target_warehouse.connect()

        if tables == None:
            tables = get_tables_to_transfer()

        if source_warehouse.config["cdc_strategy"] == "cdc_streams":
            try:
                source_warehouse.begin_transaction()
                for table_info in tables:
                    source_warehouse.create_cdc_stream(table_info)
                source_warehouse.commit_transaction()
            except Exception as e:
                source_warehouse.rollback_transaction()
                print(f"Error setting up CDC: {e}")
                raise

    finally:
        source_warehouse.disconnect()
        target_warehouse.disconnect()