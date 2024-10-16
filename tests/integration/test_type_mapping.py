import pytest
import pprint
import pandas as pd
from pathlib import Path
from src.config import Config
from src.warehouses.warehouse_factory import WarehouseFactory
from src.schema_sync import transfer_schema
from src.source_setup import setup_source
from tests.data_generators.snowflake.snowflake_data_generator import generate_insert_statement, generate_insert_into_select_statements, generate_no_primary_keys_data

@pytest.fixture
def test_config():
    # Load your test configuration
    return Config(config_path='tests/config/snowflake_to_duckdb.yaml')

def test_setup_process(test_config):
    # Load the CSV file
    csv_path = Path(__file__).parent.parent / 'data_generators' / 'snowflake' / 'no_primary_keys.csv'
    type_mappings = pd.read_csv(csv_path)
    table_info = {
        "database": test_config.source_config['cdc_schema'].split(".")[0], 
        "schema": test_config.source_config['cdc_schema'].split(".")[1], 
        "table": "TEST_TYPES_TABLE"}
    
    # Create source and target warehouse connections
    source_warehouse = WarehouseFactory.create_warehouse(test_config.source_type, test_config.source_config)
    target_warehouse = WarehouseFactory.create_warehouse(test_config.target_type, test_config.target_config)

    try:
        source_warehouse.connect()
        target_warehouse.connect()

        table_name = source_warehouse.get_full_table_name

        # Create test table in Snowflake

        columns = []
        for _, row in type_mappings.iterrows():
            column_name = row['column_name']
            column_type = row['column_type']
            primary_key = " PRIMARY KEY" if row.get('primary_key') == 'Y' else ""
            columns.append(f"\"{column_name}\" {column_type}{primary_key}")
        table_name = source_warehouse.get_full_table_name(table_info)
        create_table_sql = f"CREATE OR REPLACE TABLE {table_name} ({', '.join(columns)})"
        source_warehouse.execute_query(create_table_sql)
        insert_statements = generate_insert_into_select_statements(table_name, generate_no_primary_keys_data(50))
        # insert_sql = generate_insert_statement(table_name, generate_no_primary_keys_data(1))
        for statement in insert_statements:
            source_warehouse.execute_query(statement)
        
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

        # Disconnect warehouses
        source_warehouse.disconnect()
        target_warehouse.disconnect()