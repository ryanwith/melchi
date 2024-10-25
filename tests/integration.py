# tests/integration.py

import pytest
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
from src.utils.table_config import get_tables_to_transfer
from tests.config.config import get_test_tables
import os
from dotenv import load_dotenv
from pprint import pp

load_dotenv()


@pytest.fixture
def test_config():
    # Load your test configuration
    return Config(config_path='tests/config/snowflake_to_duckdb.yaml')

def recreate_roles(test_config):
    source_warehouse = WarehouseFactory.create_warehouse(test_config.source_type, test_config.source_config)
    role = source_warehouse.config["role"]
    user = source_warehouse.config["user"]
    test_tables = get_test_tables()
    melchi_role = source_warehouse.config["melchi_role"]

    # Create unique lists of schemas and databases
    databases = list(set(table["table_info"]["database"] for table in test_tables))


    try:
        # create melchi service role
        source_warehouse.connect("ACCOUNTADMIN")
        source_warehouse.execute_query(f"USE ROLE ACCOUNTADMIN;")
        source_warehouse.execute_query(f"DROP ROLE IF EXISTS {role};")
        source_warehouse.execute_query(f"CREATE ROLE {role};")
        source_warehouse.execute_query(f"GRANT ROLE {role} TO USER {user};")

        # create role for grants and alters
        source_warehouse.execute_query(f"DROP ROLE IF EXISTS {melchi_role};")
        source_warehouse.execute_query(f"CREATE ROLE {melchi_role};")
        source_warehouse.execute_query(f"GRANT ROLE {melchi_role} TO USER {user};")

        # enable permissions to create objects in the change tracking database
        source_warehouse.execute_query(f"GRANT USAGE ON DATABASE {source_warehouse.get_change_tracking_schema_full_name().split('.')[0]} TO ROLE {melchi_role};")
        source_warehouse.execute_query(f"GRANT CREATE SCHEMA ON DATABASE {source_warehouse.get_change_tracking_schema_full_name().split('.')[0]} TO ROLE {melchi_role};")

    #     for database in databases:
    #         print(database)
    #         source_warehouse.execute_query(f"GRANT USAGE ON DATABASE {database} TO ROLE {melchi_role};")
    #         source_warehouse.execute_query(f"GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {database} TO ROLE {melchi_role};")
    #         source_warehouse.execute_query(f"GRANT USAGE ON ALL SCHEMAS IN DATABASE {database} TO ROLE {melchi_role};")
    #         source_warehouse.execute_query(f"GRANT ALTER ON FUTURE TABLES IN DATABASE {database} TO ROLE {melchi_role};")
    #         source_warehouse.execute_query(f"GRANT ALTER ON ALL TABLES IN DATABASE {database} TO ROLE {melchi_role};")
    finally:
        source_warehouse.disconnect()

def grant_ownership_on_schema_query(schema, role = "ACCOUNTADMIN"):
    return f"GRANT OWNERSHIP ON SCHEMA {schema} TO ROLE {role};"

def drop_source_objects(test_config):
    source_warehouse = WarehouseFactory.create_warehouse(test_config.source_type, test_config.source_config)
    try:
        source_warehouse.connect("ACCOUNTADMIN")
        source_warehouse.begin_transaction()
        # drop cdc tracking schema
        source_warehouse.execute_query(f"GRANT ALL PRIVILEGES ON DATABASE {source_warehouse.get_change_tracking_schema_full_name().split(".")[0]} TO ROLE ACCOUNTADMIN;")
        print(f"GRANT OWNERSHIP ON SCHEMA {source_warehouse.get_change_tracking_schema_full_name()} TO ROLE ACCOUNTADMIN;")
        try:
            source_warehouse.execute_query(f"GRANT OWNERSHIP ON SCHEMA {source_warehouse.get_change_tracking_schema_full_name()} TO ROLE ACCOUNTADMIN;")
        except Exception as e:
            print(e)
        source_warehouse.execute_query(f"DROP SCHEMA IF EXISTS {source_warehouse.get_change_tracking_schema_full_name()} CASCADE;")
        test_tables = get_test_tables()
        schemas_to_drop = []
        grant_ownership_queries = []
        drop_schema_queries = []

        # get unique list of all schemas that may need to be dropped
        for table in test_tables:
            schema = table["table_info"]["schema"]
            database = table["table_info"]["database"]
            schemas_to_drop.append(f"{database}.{schema}")
        schemas_to_drop = list(set(schemas_to_drop))

        # generate grants to give account admin ownership
        # generate drop schema queries
        for schema in schemas_to_drop:
            grant_ownership_query = grant_ownership_on_schema_query(schema)
            drop_schema_query = f"DROP SCHEMA IF EXISTS {schema} CASCADE;"
            grant_ownership_queries.append(grant_ownership_query)
            drop_schema_queries.append(drop_schema_query)

        for query in grant_ownership_queries:
            try:
                source_warehouse.execute_query(query)
            except Exception as e:
                print(e)

        for query in drop_schema_queries:
            source_warehouse.execute_query(query)
        source_warehouse.commit_transaction()

    except Exception as e:
        source_warehouse.rollback_transaction()
        raise Exception(f"Drop source objects failed: {e}")

    finally:
        source_warehouse.disconnect()

def create_source_tables(test_config):
    test_tables = get_test_tables()
    
    # Create source and target warehouse connections
    source_warehouse = WarehouseFactory.create_warehouse(test_config.source_type, test_config.source_config)


    try:
        source_warehouse.connect(source_warehouse.config["data_generation_role"])
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
            create_table_sql = f"CREATE OR REPLACE TABLE {table_name} ({format_columns_for_snowflake(type_mappings)}) CHANGE_TRACKING=TRUE;"
            source_warehouse.execute_query(create_table_sql)
            change_tracking_sql = f"ALTER TABLE {table_name} SET CHANGE_TRACKING=TRUE;"
            source_warehouse.execute_query(change_tracking_sql)
            print(f"CREATED TABLE {source_warehouse.get_full_table_name(table_info)}")
    finally:

        # Disconnect warehouses
        source_warehouse.disconnect()

def create_cdc_schema(test_config):
    source_warehouse = WarehouseFactory.create_warehouse(test_config.source_type, test_config.source_config)
    try:
        source_warehouse.connect("ACCOUNTADMIN")
        source_warehouse.execute_query(f"GRANT ALL PRIVILEGES ON DATABASE {source_warehouse.get_change_tracking_schema_full_name().split(".")[0]} TO ROLE ACCOUNTADMIN;")
        source_warehouse.execute_query(f"CREATE SCHEMA IF NOT EXISTS {source_warehouse.get_change_tracking_schema_full_name()}")
    finally:
        source_warehouse.disconnect()

def grant_permissions_and_alter_objects(test_config):
    source_warehouse = WarehouseFactory.create_warehouse(test_config.source_type, test_config.source_config)
    tables = []
    for object in get_test_tables():
        tables.append(object["table_info"])

    try:
        source_warehouse.connect("ACCOUNTADMIN")
        grants_and_alters = source_warehouse.generate_source_sql(tables)
        for grant in grants_and_alters.split("\n"):
            if grant.strip() and not grant.strip().startswith('--'):
                source_warehouse.execute_query(grant)
    finally:
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
            insert_statements = generate_insert_into_select_statements(table_name, generate_snowflake_data(10))
            # insert_sql = generate_insert_statement(table_name, generate_snowflake_data(1))
            for statement in insert_statements:
                source_warehouse.execute_query(statement)

            print(f"Seeded data into {source_warehouse.get_full_table_name(table_info)}")

    finally:

        # Disconnect warehouses
        source_warehouse.disconnect()

def update_records(test_config, num = 5):
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
                if fate > num * 0.6:
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

@pytest.mark.depends(on=['test_initial_data_sync'])
def confirm_standard_stream_sync(test_config):
    source_warehouse = WarehouseFactory.create_warehouse(test_config.source_type, test_config.source_config)
    target_warehouse = WarehouseFactory.create_warehouse(test_config.target_type, test_config.target_config)
    
    source_warehouse.connect()
    target_warehouse.connect()
    
    tables_to_transfer = get_tables_to_transfer(test_config)
    for table_info in tables_to_transfer:
        source_table_name = source_warehouse.get_full_table_name(table_info)
        target_table_name = target_warehouse.get_full_table_name(table_info)
        
        source_df = source_warehouse.get_data_as_df(f"SELECT * FROM {source_table_name} ORDER BY 1")
        target_df = target_warehouse.get_data_as_df(f"SELECT * FROM {target_table_name} ORDER BY 1")
        
        # Convert all columns to strings for comparison
        source_df = source_df.astype(str)
        target_df = target_df.astype(str)
        
        # Compare DataFrames
        assert source_df.equals(target_df), f"Data mismatch for table {source_table_name}"

@pytest.mark.depends(on=['test_initial_data_sync'])
def confirm_append_only_stream_sync(test_config):
    """Confirms that all tables with append-only streams have all records in DuckDB, including deleted ones."""
    source_warehouse = WarehouseFactory.create_warehouse(test_config.source_type, test_config.source_config)
    target_warehouse = WarehouseFactory.create_warehouse(test_config.target_type, test_config.target_config)
    
    try:
        source_warehouse.connect()
        target_warehouse.connect()
        
        test_tables = get_test_tables()
        for table in test_tables:
            if table["cdc_type"] == "APPEND_ONLY_STREAM":
                table_info = table["table_info"]
                source_table_name = source_warehouse.get_full_table_name(table_info)
                target_table_name = target_warehouse.get_full_table_name(table_info)
                
                primary_keys = source_warehouse.get_primary_keys(table_info)
                
                source_df = source_warehouse.get_data_as_df(f"SELECT * FROM {source_table_name} ORDER BY 1")
                target_df = target_warehouse.get_data_as_df(f"SELECT * FROM {target_table_name} ORDER BY 1")
                
                if not primary_keys:
                    # Check if melchi_row_id exists in target and not in source
                    assert any(col.lower() == 'melchi_row_id' for col in target_df.columns), f"melchi_row_id should be present in target table {target_table_name}"
                    assert 'melchi_row_id' not in source_df.columns, f"melchi_row_id should not be present in source table {source_table_name}"
                    
                    # Remove melchi_row_id from target_df for comparison
                    comparison_columns = [col for col in target_df.columns if col.lower() != 'melchi_row_id']
                else:
                    # Ensure melchi_row_id is not present in either table
                    assert 'melchi_row_id' not in source_df.columns, f"melchi_row_id should not be present in source table {source_table_name}"
                    assert 'melchi_row_id' not in target_df.columns, f"melchi_row_id should not be present in target table {target_table_name}"
                    comparison_columns = target_df.columns
                
                # Check if all source records are in the target
                assert source_df.shape[0] <= target_df.shape[0], f"Target table {target_table_name} should have at least as many records as the source table."
                
                # Check if all source records are present in the target
                merged_df = pd.merge(source_df, target_df[comparison_columns], how='left', indicator=True)
                assert (merged_df['_merge'] == 'both').all(), f"Some records from source table {source_table_name} are missing in the target table."
                
                print(f"Table {source_table_name} (append-only) has all records preserved in the target, including deleted ones.")
    finally:
        source_warehouse.disconnect()
        target_warehouse.disconnect()

@pytest.mark.first
def test_prep(test_config):
    """First test to run - sets up initial test data"""
    print("Recreating roles")
    recreate_roles(test_config)
    print("Dropping source objects")
    drop_source_objects(test_config)
    print("Creating source tables")
    create_source_tables(test_config)
    print("Creating cdc schema")
    create_cdc_schema(test_config)
    print("Granting permissions and altering objects")
    grant_permissions_and_alter_objects(test_config)
    print("Inserting generated data")
    insert_generated_data(test_config)

@pytest.mark.depends(on=['test_prep'])
def test_setup_source(test_config, request):
    """Depends on successful database seeding"""
    if request.session.testsfailed:
        pytest.skip("Skipping as previous tests failed")
    setup_source(test_config)

@pytest.mark.depends(on=['test_setup_source'])
def test_transfer_schema(test_config, request):
    """Depends on successful source setup"""
    if request.session.testsfailed:
        pytest.skip("Skipping as previous tests failed")
    transfer_schema(test_config)

@pytest.mark.depends(on=['test_transfer_schema'])
def test_initial_data_sync(test_config, request):
    """Depends on successful schema transfer"""
    if request.session.testsfailed:
        pytest.skip("Skipping as previous tests failed")
    sync_data(test_config)

@pytest.mark.depends(on=['test_initial_data_sync'])
def test_cdc(test_config, request):
    """Depends on successful initial data sync"""
    if request.session.testsfailed:
        pytest.skip("Skipping as previous tests failed")
    update_records(test_config)
    sync_data(test_config)    
    # confirm_standard_stream_sync(test_config)
    # confirm_append_only_stream_sync(test_config)

# def test_tests(test_config):
#     source_warehouse = WarehouseFactory.create_warehouse(test_config.source_type, test_config.source_config)
#     source_warehouse.connect()
#     source_query = "SELECT date_test_col, datetime_test_col FROM MELCHI_TEST_DATA.TEST_MELCHI_SCHEMA.NO_PK_APPEND_ONLY_STREAM limit 10;"
#     source_df = source_warehouse.get_data_as_df(source_query)
#     target_warehouse = WarehouseFactory.create_warehouse(test_config.target_type, test_config.target_config)
#     target_query = "SELECT date_test_col, datetime_test_col FROM TEST_MELCHI_SCHEMA.NO_PK_APPEND_ONLY_STREAM limit 10;"
#     target_df = target_warehouse.get_data_as_df(target_query)
#     pp(source_df)
#     pp(target_df)