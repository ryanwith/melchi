# src/data_sync.py

from .warehouses.warehouse_factory import WarehouseFactory
from .data_ingestion_manager import DataIngestionManager
from .utils.table_config import get_tables_to_transfer


def sync_data(config):
    source_warehouse = WarehouseFactory.create_warehouse(config.source_type, config.source_config)
    target_warehouse = WarehouseFactory.create_warehouse(config.target_type, config.target_config)
    data_ingestion_manager = DataIngestionManager(source_warehouse, target_warehouse)

    tables_to_transfer = get_tables_to_transfer(config)

    for table_info in tables_to_transfer:
        data_ingestion_manager.sync_table(table_info)   