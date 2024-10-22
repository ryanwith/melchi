import pytest
from unittest.mock import Mock, patch, call
from src.warehouses.duckdb_warehouse import DuckDBWarehouse

@pytest.fixture
def config():
    return {
        'database': 'test_db.duckdb',
        'change_tracking_schema': 'cdc_schema',
        'warehouse_role': 'TARGET',
        'replace_existing': True
    }

@pytest.fixture
def warehouse(config):
    with patch('duckdb.connect') as mock_connect:
        warehouse = DuckDBWarehouse(config)
        # Set up the mock connection
        mock_connection = mock_connect.return_value
        warehouse.connection = mock_connection
        return warehouse

# Connection Tests
def test_connect(warehouse):
    with patch('duckdb.connect') as mock_connect:
        warehouse.connect()
        mock_connect.assert_called_once_with(warehouse.config['database'])

def test_disconnect(config):
    with patch('duckdb.connect') as mock_connect:
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        warehouse = DuckDBWarehouse(config)
        warehouse.connection = mock_connection
        warehouse.disconnect()
        
        mock_connection.close.assert_called_once()
        assert warehouse.connection == None

def test_connect_error(warehouse):
    with patch('duckdb.connect', side_effect=Exception("Connection failed")):
        with pytest.raises(Exception, match="Connection failed"):
            warehouse.connect()

def test_disconnect_when_not_connected(warehouse):
    warehouse.connection = None
    warehouse.disconnect()  # Should not raise any error

# Transaction Management Tests
def test_begin_and_commit_transaction(warehouse):
    with patch.object(warehouse.connection, 'begin') as mock_begin:
        with patch.object(warehouse.connection, 'commit') as mock_commit:
            warehouse.begin_transaction()
            warehouse.commit_transaction()
            
            mock_begin.assert_called_once()
            mock_commit.assert_called_once()

def test_rollback_transaction(warehouse):
    with patch.object(warehouse.connection, 'rollback') as mock_rollback:
        warehouse.rollback_transaction()
        mock_rollback.assert_called_once()

# Schema and Table Management Tests
def test_get_full_table_name(warehouse):
    table_info = {'schema': 'test_schema', 'table': 'test_table'}
    expected = "test_schema.test_table"
    assert warehouse.get_full_table_name(table_info) == expected

def test_replace_existing_tables(warehouse):
    assert warehouse.replace_existing_tables() == True

def test_format_schema_row(warehouse):
    input_row = ('col_id', 'col_name', 'INTEGER', 'TRUE', 'NULL', 'TRUE')
    expected = {
        'name': 'col_name',
        'type': 'INTEGER',
        'nullable': True,
        'default_value': 'NULL',
        'primary_key': True,
    }
    assert warehouse.format_schema_row(input_row) == expected

def test_generate_create_table_statement(warehouse):
    table_info = {'schema': 'test_schema', 'table': 'test_table'}
    schema = [
        {'name': 'id', 'type': 'INTEGER', 'nullable': False, 'default_value': None, 'primary_key': True},
        {'name': 'name', 'type': 'VARCHAR', 'nullable': True, 'default_value': None, 'primary_key': False}
    ]
    expected = "CREATE OR REPLACE TABLE test_schema.test_table (id INTEGER NOT NULL, name VARCHAR);"
    assert warehouse.generate_create_table_statement(table_info, schema) == expected

# Change Tracking Tests
def test_get_change_tracking_schema_full_name(warehouse):
    assert warehouse.get_change_tracking_schema_full_name() == warehouse.config['change_tracking_schema']

def test_setup_target_environment_with_replace(warehouse):
    expected_calls = [
        f"CREATE SCHEMA IF NOT EXISTS {warehouse.config['change_tracking_schema']};",
        f"""CREATE OR REPLACE TABLE {warehouse.config['change_tracking_schema']}.captured_tables (schema_name varchar, table_name varchar, created_at timestamp, updated_at timestamp, primary_keys varchar[]);""",
        f"""CREATE OR REPLACE TABLE {warehouse.config['change_tracking_schema']}.source_columns (table_catalog varchar, table_schema varchar, table_name varchar, column_name varchar, data_type varchar, column_default varchar, is_nullable boolean, primary_key boolean);"""
    ]
    
    with patch.object(warehouse.connection, 'execute') as mock_execute:
        warehouse.setup_target_environment()
        
        assert mock_execute.call_count == 3
        actual_calls = [call[0][0].strip() for call in mock_execute.call_args_list]
        for expected, actual in zip(expected_calls, actual_calls):
            assert expected.strip() == actual

def test_setup_target_environment_without_replace(config):
    config['replace_existing'] = False
    with patch('duckdb.connect') as mock_connect:
        warehouse = DuckDBWarehouse(config)
        mock_connection = mock_connect.return_value
        warehouse.connection = mock_connection

        expected_calls = [
            f"CREATE SCHEMA IF NOT EXISTS {config['change_tracking_schema']};",
            f"""CREATE TABLE IF NOT EXISTS {config['change_tracking_schema']}.captured_tables (schema_name varchar, table_name varchar, created_at timestamp, updated_at timestamp, primary_keys varchar[]);""",
            f"""CREATE TABLE IF NOT EXISTS {config['change_tracking_schema']}.source_columns (table_catalog varchar, table_schema varchar, table_name varchar, column_name varchar, data_type varchar, column_default varchar, is_nullable boolean, primary_key boolean);"""
        ]
        
        with patch.object(warehouse.connection, 'execute') as mock_execute:
            warehouse.setup_target_environment()
            
            assert mock_execute.call_count == 3
            actual_calls = [call[0][0].strip() for call in mock_execute.call_args_list]
            for expected, actual in zip(expected_calls, actual_calls):
                assert expected.strip() == actual

# Data Synchronization Tests
def test_sync_table_operations(warehouse):
    """Test the complete sync operation including temp table, deletes, inserts and cleanup."""
    table_info = {'schema': 'test_schema', 'table': 'test_table'}
    mock_df = Mock()
    
    schema_columns = [
        {'name': 'id', 'type': 'INTEGER', 'nullable': False, 'default_value': None, 'primary_key': True},
        {'name': 'name', 'type': 'VARCHAR', 'nullable': True, 'default_value': None, 'primary_key': False},
    ]
    
    with patch.object(warehouse, 'get_schema', return_value=schema_columns):
        with patch.object(warehouse, 'get_primary_keys', return_value=['id']):
            with patch.object(warehouse.connection, 'execute') as mock_execute:
                warehouse.sync_table(table_info, mock_df)
                
                expected_calls = [
                    f"CREATE OR REPLACE TEMP TABLE test_table_melchi_cdc AS (SELECT * FROM df)",
                    """DELETE FROM test_schema.test_table
                        WHERE (id) IN 
                        (
                            SELECT (id)
                            FROM test_table_melchi_cdc
                            WHERE melchi_metadata_action = 'DELETE'
                        );
                    """.strip(),
                    """INSERT INTO test_schema.test_table
                        SELECT id, name FROM test_table_melchi_cdc
                        WHERE melchi_metadata_action = 'INSERT'
                    """.strip(),
                    "UPDATE cdc_schema.captured_tables SET updated_at = current_timestamp WHERE table_name = 'test_table' and schema_name = 'test_schema' ",
                    "DROP TABLE test_table_melchi_cdc"
                ]
                
                assert mock_execute.call_count == 5
                actual_calls = [args[0] for args, kwargs in mock_execute.call_args_list]
                
                for expected, actual in zip(expected_calls, actual_calls):
                    expected_sql = ' '.join(expected.split())
                    actual_sql = ' '.join(actual.split())
                    assert expected_sql == actual_sql

def test_sync_table_without_primary_keys(warehouse):
    """Test sync operation when table has no primary keys."""
    table_info = {'schema': 'test_schema', 'table': 'test_table'}
    mock_df = Mock()
    
    schema_columns = [
        {'name': 'name', 'type': 'VARCHAR', 'nullable': True, 'default_value': None, 'primary_key': False},
        {'name': 'value', 'type': 'INTEGER', 'nullable': True, 'default_value': None, 'primary_key': False},
    ]
    
    with patch.object(warehouse, 'get_schema', return_value=schema_columns):
        with patch.object(warehouse, 'get_primary_keys', return_value=['MELCHI_ROW_ID']):
            with patch.object(warehouse.connection, 'execute') as mock_execute:
                warehouse.sync_table(table_info, mock_df)
                
                expected_calls = [
                    f"CREATE OR REPLACE TEMP TABLE test_table_melchi_cdc AS (SELECT * FROM df)",
                    """DELETE FROM test_schema.test_table
                        WHERE (MELCHI_ROW_ID) IN 
                        (
                            SELECT (MELCHI_ROW_ID)
                            FROM test_table_melchi_cdc
                            WHERE melchi_metadata_action = 'DELETE'
                        );
                    """.strip(),
                    """INSERT INTO test_schema.test_table
                        SELECT name, value FROM test_table_melchi_cdc
                        WHERE melchi_metadata_action = 'INSERT'
                    """.strip(),
                    "UPDATE cdc_schema.captured_tables SET updated_at = current_timestamp WHERE table_name = 'test_table' and schema_name = 'test_schema' ",
                    "DROP TABLE test_table_melchi_cdc"
                ]
                
                assert mock_execute.call_count == 5
                actual_calls = [args[0] for args, kwargs in mock_execute.call_args_list]
                
                for expected, actual in zip(expected_calls, actual_calls):
                    expected_sql = ' '.join(expected.split())
                    actual_sql = ' '.join(actual.split())
                    assert expected_sql == actual_sql

def test_get_primary_keys(warehouse):
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = (['pk1', 'pk2'],)
    
    with patch.object(warehouse.connection, 'execute', return_value=mock_cursor) as mock_execute:
        table_info = {'schema': 'test_schema', 'table': 'test_table'}
        result = warehouse.get_primary_keys(table_info)
        
        assert result == ['pk1', 'pk2']
        expected_sql = """
            SELECT primary_keys FROM cdc_schema.captured_tables
                WHERE table_name = 'test_table' and schema_name = 'test_schema'
        """.strip()
        mock_execute.assert_called_once_with(expected_sql)

# Utility Method Tests
def test_convert_list_to_duckdb_syntax(warehouse):
    input_list = ['col1', 'col2', 'col3']
    expected = "['col1', 'col2', 'col3']"
    assert warehouse.convert_list_to_duckdb_syntax(input_list) == expected