from .snowflake_warehouse import SnowflakeWarehouse
from .duckdb_warehouse import DuckDBWarehouse

class WarehouseFactory:
    @staticmethod
    def create_warehouse(warehouse_type, config):
        if warehouse_type == 'snowflake':
            return SnowflakeWarehouse(config)
        elif warehouse_type == 'duckdb':
            return DuckDBWarehouse(config)
        else:
            raise ValueError(f"Unsupported warehouse type: {warehouse_type}")
