from .warehouses.warehouse_factory import WarehouseFactory

def sync_data(config):
    source_warehouse = WarehouseFactory.create_warehouse(config.source_type, config.source_config)
    target_warehouse = WarehouseFactory.create_warehouse(config.target_type, config.target_config)

    try:
        source_warehouse.connect()
        target_warehouse.connect()

        tables = get_tables_to_transfer(source_warehouse)

        # setup source warehouse
        # for table in tables:
        #     print(table)

        # # setup target warehouse 
        # for table in tables:
        #     source_schema = source_warehouse.get_schema(table)
            
        #     # Map source schema to target schema
        #     target_schema = [
        #         (col_name, source_warehouse.map_type_to(config.target_type, col_type))
        #         for col_name, col_type in source_schema
        #     ]

        #     target_warehouse.create_table(table, target_schema)

        #     data = source_warehouse.get_data(table)
        #     target_warehouse.insert_data(table, data)

    finally:
        source_warehouse.disconnect()
        target_warehouse.disconnect()

def get_tables_to_transfer(warehouse):
    # Implementation to get list of tables to transfer
    pass