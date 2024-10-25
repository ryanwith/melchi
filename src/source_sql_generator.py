# src/source_sql_generator.py

from src.utils.table_config import get_tables_to_transfer
from .warehouses.warehouse_factory import WarehouseFactory

def generate_source_sql(config, file_location):
    if config.source_type == "snowflake":
        source_warehouse = WarehouseFactory.create_warehouse(config.source_type, config.source_config)
        tables = get_tables_to_transfer(config)
        sql = source_warehouse.generate_source_sql(tables)
        write_sql_to_file(sql, f"{file_location}/source_setup.sql")
        return sql
    else:
        print(f"{config.source_type} is not yet supported as a source")


def write_sql_to_file(permissions, file_name):
    with open(file_name, 'w') as f:
        f.write(permissions)
