# src/source_setup.py

from .warehouses.warehouse_factory import WarehouseFactory
from .utils.table_config import get_tables_to_transfer

def setup_source(config, tables_to_transfer = None):
    source_warehouse = WarehouseFactory.create_warehouse(config.source_type, config.source_config)

    try:
        tables_to_transfer = get_tables_to_transfer(config)

        if source_warehouse.config["cdc_strategy"] == "cdc_streams":
            try:
                source_warehouse.connect()
                source_warehouse.begin_transaction()
                source_warehouse.setup_environment(tables_to_transfer)
                source_warehouse.commit_transaction()
            except Exception as e:
                source_warehouse.rollback_transaction()
                print(f"Error setting up source_warehouse: {e}")
                raise

    finally:
        source_warehouse.disconnect()
