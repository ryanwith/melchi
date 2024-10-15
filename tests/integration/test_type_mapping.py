import pytest
import pprint
import pandas as pd
from pathlib import Path
from src.config import Config
from src.warehouses.warehouse_factory import WarehouseFactory
from src.schema_sync import transfer_schema
from src.source_setup import setup_source

@pytest.fixture
def test_config():
    # Load your test configuration
    return Config(config_path='tests/config/snowflake_to_duckdb.yaml')

def test_setup_process(test_config):
    # Load the CSV file
    csv_path = Path(__file__).parent.parent / 'type_mappings' / 'snowflake_to_duckdb' / 'no_primary_keys.csv'
    type_mappings = pd.read_csv(csv_path)
    schema = test_config.source_config['cdc_schema']
    table = 'TEST_TYPES_TABLE'
    table_name = f"{schema}.{table}"

    # Create source and target warehouse connections
    source_warehouse = WarehouseFactory.create_warehouse(test_config.source_type, test_config.source_config)
    target_warehouse = WarehouseFactory.create_warehouse(test_config.target_type, test_config.target_config)

    try:
        source_warehouse.connect()
        target_warehouse.connect()

        # Create test table in Snowflake
        columns = []
        for _, row in type_mappings.iterrows():
            column_name = row['column_name']
            column_type = row['column_type']
            primary_key = " PRIMARY KEY" if row.get('primary_key') == 'Y' else ""
            columns.append(f"\"{column_name}\" {column_type}{primary_key}")
        source_warehouse.execute_query("SELECT current_role();")
        create_table_sql = f"CREATE OR REPLACE TABLE {table_name} ({', '.join(columns)})"
        source_warehouse.execute_query(create_table_sql)

        # Disconnect from warehouses
        source_warehouse.disconnect()
        target_warehouse.disconnect()
        
        # Run setup_source
        setup_source(test_config)

        # Run transfer_schema
        transfer_schema(test_config)

        # # Verify the table structure in the target warehouse
        # target_table_info = target_warehouse.get_table_info(table_name)
        
        # for _, row in type_mappings.iterrows():
        #     column_name = row['column_name']
        #     expected_type = row['type']
        #     actual_type = target_table_info.get(column_name)
        #     assert actual_type is not None, f"Column {column_name} not found in target table"
        #     assert actual_type.lower() == expected_type.lower(), f"Type mismatch for column {column_name}. Expected: {expected_type}, Got: {actual_type}"

    finally:

        source_warehouse.connect()
        target_warehouse.connect()
        print (table_name)
        # # Clean up: Drop the test table from both source and target
        # source_warehouse.execute_query(f"DROP TABLE IF EXISTS {table_name}")
        # target_warehouse.execute_query(f"DROP TABLE IF EXISTS {table_name}")

        # Disconnect warehouses
        source_warehouse.disconnect()
        target_warehouse.disconnect()