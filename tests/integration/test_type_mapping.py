# tests/integration/test_type_mapping.py

import pytest
import random
import pandas as pd
from pathlib import Path
from src.config import Config
from src.warehouses.warehouse_factory import WarehouseFactory
from src.schema_sync import transfer_schema
from src.source_setup import setup_source
from src.data_sync import sync_data
from tests.data_generators.snowflake.snowflake_data_generator import (
    generate_insert_into_select_statements, 
    generate_snowflake_data, 
    format_columns_for_snowflake, 
    get_random_records_sql,
    generate_delete_query,
    generate_update_query
)
from tests.config.config import get_test_tables

@pytest.fixture
def test_config():
    # Load your test configuration
    return Config(config_path='tests/config/snowflake_to_duckdb.yaml')

def create_source_tables(test_config):
    test_tables = get_test_tables()
    
    # Create source and target warehouse connections
    source_warehouse = WarehouseFactory.create_warehouse(test_config.source_type, test_config.source_config)


    try:
        source_warehouse.connect()
        source_warehouse.execute_query(f"USE ROLE {source_warehouse.config["data_generation_role"]};")
        source_warehouse.execute_query(f"USE DATABASE {source_warehouse.config["database"]};")

        for table in test_tables:
            csv_path = Path(__file__).parent.parent / table["schema_location"]
            type_mappings = pd.read_csv(csv_path)
            table_info = table["table_info"]
            table_name = source_warehouse.get_full_table_name
            # Create test table in Snowflake
            table_name = source_warehouse.get_full_table_name(table_info)
            create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {table_info["schema"]}"
            source_warehouse.execute_query(create_schema_sql)
            create_table_sql = f"CREATE OR REPLACE TABLE {table_name} ({format_columns_for_snowflake(type_mappings)})"
            source_warehouse.execute_query(create_table_sql)
            change_tracking_sql = f"ALTER TABLE {table_name} SET CHANGE_TRACKING=TRUE;"
            source_warehouse.execute_query(change_tracking_sql)
    finally:

        # Disconnect warehouses
        source_warehouse.disconnect()

def insert_generated_data(test_config):
    test_tables = get_test_tables()
    
    # Create source and target warehouse connections
    source_warehouse = WarehouseFactory.create_warehouse(test_config.source_type, test_config.source_config)

    try:
        source_warehouse.connect()
        source_warehouse.execute_query(f"USE ROLE {source_warehouse.config["data_generation_role"]};")
        source_warehouse.execute_query(f"USE DATABASE {source_warehouse.config["database"]};")


        for table in test_tables:
            table_info = table["table_info"]
            table_name = source_warehouse.get_full_table_name(table_info)
            insert_statements = generate_insert_into_select_statements(table_name, generate_snowflake_data(20))
            # insert_sql = generate_insert_statement(table_name, generate_snowflake_data(1))
            for statement in insert_statements:
                source_warehouse.execute_query(statement)

    finally:

        # Disconnect warehouses
        source_warehouse.disconnect()

def update_records(test_config, num = 10):
    test_tables = get_test_tables()
    
    # Create source and target warehouse connections
    source_warehouse = WarehouseFactory.create_warehouse(test_config.source_type, test_config.source_config)

    try:
        source_warehouse.connect()
        source_warehouse.execute_query(f"USE ROLE {source_warehouse.config["data_generation_role"]}")
        source_warehouse.begin_transaction()
        queries_to_execute = []

        for table in test_tables:

            table_info = table["table_info"]
            table_name = source_warehouse.get_full_table_name(table_info)

            random_records = source_warehouse.execute_query(get_random_records_sql(table_name, num), True)
            
            to_be_deleted = []
            to_be_updated = []
            for fate in range(num):
                if fate > 6:
                    to_be_deleted.append(random_records[fate][0])
                else:
                    to_be_updated.append(random_records[fate][0])
            delete_query = generate_delete_query(table_name, to_be_deleted, "$1")
            update_query = generate_update_query(table_name, to_be_updated, "timestamp_ltz_test_col", "current_timestamp", "$1")
            insert_queries = generate_insert_into_select_statements(table_name, generate_snowflake_data(5))

            queries_to_execute += insert_queries + [delete_query, update_query]
        
        for query in queries_to_execute:
            source_warehouse.execute_query(query)

        source_warehouse.commit_transaction()
    except Exception as e:
        source_warehouse.rollback_transaction()
        print(f"Error changing test data: {e}")
        raise

    finally:
        # Disconnect warehouses
        source_warehouse.disconnect()

def test_seed_db(test_config):
    create_source_tables(test_config)
    insert_generated_data(test_config)

def test_setup_source(test_config):
    setup_source(test_config)

def test_transfer_schema(test_config):
    transfer_schema(test_config)

def test_initial_data_sync(test_config):
    sync_data(test_config)
    
def test_cdc(test_config):
    update_records(test_config)
    sync_data(test_config)