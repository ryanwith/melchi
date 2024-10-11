from .warehouses.warehouse_factory import WarehouseFactory
from .utils.table_config import get_tables_to_transfer

def transfer_schema(config, tables=None):
    source_warehouse = WarehouseFactory.create_warehouse(config.source_type, config.source_config)
    target_warehouse = WarehouseFactory.create_warehouse(config.target_type, config.target_config)

    try:
        source_warehouse.connect()
        target_warehouse.connect()

        if tables is None:
            tables = get_tables_to_transfer()

        try:
            target_warehouse.begin_transaction()
            # creates the tables needed for tracking CDC
            target_warehouse.setup_target_environment()
            for table_info in tables:
                full_table_name = f"{table_info['database']}.{table_info['schema']}.{table_info['table']}"
                source_schema = source_warehouse.get_schema(full_table_name)
                
                target_schema = generate_target_schema(source_warehouse, source_schema, config.target_type)

                target_warehouse.create_table(table_info['schema'], table_info['table'], target_schema)

                # Optionally, you could also insert initial data here
                # initial_data = source_warehouse.get_data(full_table_name)
                # target_warehouse.insert_data(target_table_name, initial_data)

                print(f"Table {full_table_name} schema transferred successfully.")

            target_warehouse.commit_transaction()
        except Exception as e:
            target_warehouse.rollback_transaction()
            print(f"Error during schema sync: {e}")
            raise
    finally:
        source_warehouse.disconnect()
        target_warehouse.disconnect()

# generates a target schema based on the source schema
def generate_target_schema(source_warehouse, source_schema, target_type):
    target_schema = [
        (col_name, source_warehouse.map_type_to(target_type, col_type), primary_key)
        for col_name, col_type, primary_key in source_schema
    ]
    return target_schema