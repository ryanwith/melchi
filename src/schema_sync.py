# src/schema_sync.py

from .warehouses.warehouse_factory import WarehouseFactory
from .utils.table_config import get_tables_to_transfer
from pprint import pp

def transfer_schema(config, tables=None):
    source_warehouse = WarehouseFactory.create_warehouse(config.source_type, config.source_config)
    target_warehouse = WarehouseFactory.create_warehouse(config.target_type, config.target_config)
    try:
        source_warehouse.connect()
        target_warehouse.connect()
        if tables is None:
            tables = get_tables_to_transfer(config)
        try:
            target_warehouse.begin_transaction()
            # creates the tables needed for tracking CDC
            target_warehouse.setup_environment()
            for table_info in tables:
                source_schema = source_warehouse.get_schema(table_info)
                target_schema = source_warehouse.map_schema_to(table_info, config.target_type)
                target_warehouse.create_table(table_info, source_schema, target_schema )
            target_warehouse.commit_transaction()
        except Exception as e:
            target_warehouse.rollback_transaction()
            print(f"Error during schema sync: {e}")
            raise
    finally:
        source_warehouse.disconnect()
        target_warehouse.disconnect()