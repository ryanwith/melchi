from .warehouses.warehouse_factory import WarehouseFactory
from .utils.table_config import get_tables_to_transfer


def sync_data(config):
    source_warehouse = WarehouseFactory.create_warehouse(config.source_type, config.source_config)
    target_warehouse = WarehouseFactory.create_warehouse(config.target_type, config.target_config)

    try:
        source_warehouse.connect()
        target_warehouse.connect()

        tables = get_tables_to_transfer()

        if source_warehouse.config["cdc_strategy"] == "cdc_streams":
            for table_info in tables:
                try:
                    target_warehouse.begin_transaction()
                    cdc_df = source_warehouse.get_cdc_data(table_info)
                    target_warehouse.sync_table(table_info, cdc_df)
                    target_warehouse.commit_transaction()
                    source_warehouse.cleanup_cdc_for_table(table_info)

                except Exception as e:
                    source_warehouse.rollback_transaction()
                    target_warehouse.rollback_transaction()
                    print(f"Error ingesting data sync: {e}")
                    raise
                # while True:
                #     batch = snowflake_cursor.fetchmany(source_warehouse.config["batch_size"])
                #     if not batch:
                #         break
                    
                #     df = pd.DataFrame(batch, columns=[desc[0] for desc in snowflake_cursor.description])
                    
                #     # Use DuckDB's from_df function to insert data from the DataFrame
                #     duckdb_conn.execute(f"INSERT INTO {table_name} SELECT * FROM duckdb.from_df($df)", {'df': df})
        # setup source warehouse
        # for table in tables:

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

def transfer_data(snowflake_cursor, duckdb_conn, table_name, batch_size=10000):
    snowflake_cursor.execute(f"SELECT * FROM {table_name}")
    
    while True:
        batch = snowflake_cursor.fetchmany(batch_size)
        if not batch:
            break
        
        df = pd.DataFrame(batch, columns=[desc[0] for desc in snowflake_cursor.description])
        
        # Use DuckDB's from_df function to insert data from the DataFrame
        duckdb_conn.execute(f"INSERT INTO {table_name} SELECT * FROM duckdb.from_df($df)", {'df': df})