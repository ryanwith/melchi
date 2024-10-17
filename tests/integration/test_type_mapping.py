import pytest
import pprint
import pandas as pd
from pathlib import Path
from src.config import Config
from src.warehouses.warehouse_factory import WarehouseFactory
from src.schema_sync import transfer_schema
from src.source_setup import setup_source
from src.data_sync import sync_data
from tests.data_generators.snowflake.snowflake_data_generator import generate_insert_statement, generate_insert_into_select_statements, generate_no_primary_keys_data

@pytest.fixture
def test_config():
    # Load your test configuration
    return Config(config_path='tests/config/snowflake_to_duckdb.yaml')

def get_test_tables(test_config):
    test_tables = [{
        "schema_location": "data_generators/snowflake/no_primary_keys.csv",
        "table_info": {
            "database": test_config.source_config['cdc_schema'].split(".")[0], 
            "schema": test_config.source_config['cdc_schema'].split(".")[1], 
            "table": "TEST_TYPES_TABLE"}
    }]
    return test_tables

def create_source_tables(test_config):
    test_tables = get_test_tables(test_config)
    
    # Create source and target warehouse connections
    source_warehouse = WarehouseFactory.create_warehouse(test_config.source_type, test_config.source_config)
    target_warehouse = WarehouseFactory.create_warehouse(test_config.target_type, test_config.target_config)

    try:
        source_warehouse.connect()
        target_warehouse.connect()

        for table in test_tables:
            csv_path = Path(__file__).parent.parent / table["schema_location"]
            type_mappings = pd.read_csv(csv_path)
            table_info = table["table_info"]

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

    finally:

        # Disconnect warehouses
        source_warehouse.disconnect()
        target_warehouse.disconnect()

def insert_generated_data(test_config):
    test_tables = get_test_tables(test_config)
    
    # Create source and target warehouse connections
    source_warehouse = WarehouseFactory.create_warehouse(test_config.source_type, test_config.source_config)
    target_warehouse = WarehouseFactory.create_warehouse(test_config.target_type, test_config.target_config)

    try:
        source_warehouse.connect()
        target_warehouse.connect()

        for table in test_tables:
            table_info = table["table_info"]
            table_name = source_warehouse.get_full_table_name(table_info)
            insert_statements = generate_insert_into_select_statements(table_name, generate_no_primary_keys_data(5))
            # insert_sql = generate_insert_statement(table_name, generate_no_primary_keys_data(1))
            for statement in insert_statements:
                source_warehouse.execute_query(statement)

    finally:

        # Disconnect warehouses
        source_warehouse.disconnect()
        target_warehouse.disconnect()


def test_setup_source(test_config):
    create_source_tables(test_config)
    insert_generated_data(test_config)
    setup_source(test_config)

def test_transfer_schema(test_config):
    transfer_schema(test_config)

def test_sync_data(test_config):
    sync_data(test_config)