import pytest
from unittest.mock import Mock, patch, call
from src.warehouses.duckdb_warehouse import DuckDBWarehouse
from datetime import datetime
from tests.utils.helpers import normalize_sql
from src.utils.table_config import get_cdc_type

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
        # Set up mock connection with execute method
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        # Call connect
        warehouse.connect()
        
        # Verify connection is created with correct database
        mock_connect.assert_called_once_with(warehouse.config['database'])
        
        # Verify spatial extension commands are executed in correct order
        expected_calls = [
            call("INSTALL spatial;"),
            call("LOAD spatial;")
        ]
        mock_connection.execute.assert_has_calls(expected_calls)
        assert mock_connection.execute.call_count == 2

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

def test_create_table_standard_stream_with_primary_keys_replace_existing(warehouse):
    table_info = {
        "database": "test_db",
        "schema": "test_schema",
        "table": "test_table",
        "cdc_type": "standard_stream"
    }
    
    source_schema = [
        {
            "name": "id",
            "type": "INTEGER",
            "nullable": False,
            "default_value": None,
            "primary_key": True
        },
        {
            "name": "name",
            "type": "VARCHAR",
            "nullable": True,
            "default_value": None,
            "primary_key": False
        }
    ]

    cdc_type = get_cdc_type(table_info)
    
    target_schema = source_schema.copy()
    
    with patch.object(warehouse.connection, 'execute') as mock_execute:
        # Mock datetime to get consistent timestamp
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 10, 23, 17, 6, 55)
            warehouse.create_table(table_info, source_schema, target_schema)
            
            expected_calls = [
                f"CREATE SCHEMA IF NOT EXISTS {table_info['schema']};",
                
                f"CREATE OR REPLACE TABLE {table_info['schema']}.{table_info['table']} (id INTEGER NOT NULL, name VARCHAR);",
                
                f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.captured_tables VALUES ('test_schema', 'test_table', '2024-10-23 17:06:55', '2024-10-23 17:06:55', ['id'], '{cdc_type}');""",
                
                f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.source_columns VALUES ( 'test_db', 'test_schema', 'test_table', 'id', 'INTEGER', NULL, FALSE, TRUE ),
                ( 'test_db', 'test_schema', 'test_table', 'name', 'VARCHAR', NULL, TRUE, FALSE );"""
            ]
            
            assert mock_execute.call_count == 4
            for i, (expected, actual) in enumerate(zip(expected_calls, mock_execute.call_args_list)):
                expected_sql = normalize_sql(expected)
                actual_sql = normalize_sql(actual[0][0])  # Fix is here
                assert expected_sql == actual_sql, f"Mismatch in query {i+1}"

def test_create_table_standard_stream_no_primary_keys_replace_existing(warehouse):
    table_info = {
        "database": "test_db",
        "schema": "test_schema",
        "table": "test_table",
        "cdc_type": "standard_stream"
    }
    
    source_schema = [
        {
            "name": "id",
            "type": "INTEGER",
            "nullable": False,
            "default_value": None,
            "primary_key": False
        },
        {
            "name": "name",
            "type": "VARCHAR",
            "nullable": True,
            "default_value": None,
            "primary_key": False
        }
    ]

    cdc_type = get_cdc_type(table_info)
    
    target_schema = source_schema.copy()
    
    with patch.object(warehouse.connection, 'execute') as mock_execute:
        # Mock datetime to get consistent timestamp
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 10, 23, 17, 6, 55)
            warehouse.create_table(table_info, source_schema, target_schema)
            
            expected_calls = [
                f"CREATE SCHEMA IF NOT EXISTS {table_info['schema']};",
                
                f"CREATE OR REPLACE TABLE {table_info['schema']}.{table_info['table']} (id INTEGER NOT NULL, name VARCHAR, MELCHI_ROW_ID VARCHAR NOT NULL);",
                
                f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.captured_tables VALUES ('test_schema', 'test_table', '2024-10-23 17:06:55', '2024-10-23 17:06:55', ['MELCHI_ROW_ID'], '{cdc_type}');""",
                
                f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.source_columns VALUES ( 'test_db', 'test_schema', 'test_table', 'id', 'INTEGER', NULL, FALSE, FALSE ),
                ( 'test_db', 'test_schema', 'test_table', 'name', 'VARCHAR', NULL, TRUE, FALSE );"""
            ]
            
            assert mock_execute.call_count == 4
            for i, (expected, actual) in enumerate(zip(expected_calls, mock_execute.call_args_list)):
                expected_sql = normalize_sql(expected)
                actual_sql = normalize_sql(actual[0][0])  # Fix is here
                assert expected_sql == actual_sql, f"Mismatch in query {i+1}"

def test_create_table_non_standard_stream_with_primary_keys_replace_existing(warehouse):
    table_info = {
        "database": "test_db",
        "schema": "test_schema",
        "table": "test_table",
        "cdc_type": "append_only_stream"
    }
    
    source_schema = [
        {
            "name": "id",
            "type": "INTEGER",
            "nullable": False,
            "default_value": None,
            "primary_key": True
        },
        {
            "name": "name",
            "type": "VARCHAR",
            "nullable": True,
            "default_value": None,
            "primary_key": False
        }
    ]

    cdc_type = get_cdc_type(table_info)
    
    target_schema = source_schema.copy()
    
    with patch.object(warehouse.connection, 'execute') as mock_execute:
        # Mock datetime to get consistent timestamp
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 10, 23, 17, 6, 55)
            warehouse.create_table(table_info, source_schema, target_schema)
            
            expected_calls = [
                f"CREATE SCHEMA IF NOT EXISTS {table_info['schema']};",
                
                f"CREATE OR REPLACE TABLE {table_info['schema']}.{table_info['table']} (id INTEGER NOT NULL, name VARCHAR);",
                
                f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.captured_tables VALUES ('test_schema', 'test_table', '2024-10-23 17:06:55', '2024-10-23 17:06:55', ['id'], '{cdc_type}');""",
                
                f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.source_columns VALUES ( 'test_db', 'test_schema', 'test_table', 'id', 'INTEGER', NULL, FALSE, TRUE ),
                ( 'test_db', 'test_schema', 'test_table', 'name', 'VARCHAR', NULL, TRUE, FALSE );"""
            ]
            
            assert mock_execute.call_count == 4
            for i, (expected, actual) in enumerate(zip(expected_calls, mock_execute.call_args_list)):
                expected_sql = normalize_sql(expected)
                actual_sql = normalize_sql(actual[0][0])  # Fix is here
                assert expected_sql == actual_sql, f"Mismatch in query {i+1}"


def test_create_table_standard_stream_without_primary_keys_no_replace_existing(warehouse):
    table_info = {
        "database": "test_db",
        "schema": "test_schema",
        "table": "test_table",
        "cdc_type": "standard_stream"
    }
    
    source_schema = [
        {
            "name": "name",
            "type": "VARCHAR",
            "nullable": True,
            "default_value": None,
            "primary_key": False
        }
    ]

    warehouse.config["replace_existing"] = False
    
    target_schema = source_schema.copy()

    with patch.object(warehouse.connection, 'execute') as mock_execute:
        warehouse.create_table(table_info, source_schema, target_schema)
        
        expected_calls = [
            # Create schema
            f"CREATE SCHEMA IF NOT EXISTS {table_info['schema']};",
            
            # Create table with auto-generated primary key
            f"""CREATE TABLE IF NOT EXISTS {table_info['schema']}.{table_info['table']} (name VARCHAR, MELCHI_ROW_ID VARCHAR NOT NULL);""",
            
            # Insert metadata with auto-generated primary key
            f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.captured_tables VALUES ('test_schema', 'test_table', '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', ['MELCHI_ROW_ID'], 'STANDARD_STREAM');""",
            
            f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.source_columns VALUES ( 'test_db', 'test_schema', 'test_table', 'name', 'VARCHAR', NULL, TRUE, FALSE );"""
        ]
        
        assert mock_execute.call_count == 4
        for i, (expected, actual) in enumerate(zip(expected_calls, mock_execute.call_args_list)):
            expected_sql = normalize_sql(expected)
            actual_sql = normalize_sql(actual[0][0])
            assert expected_sql == actual_sql, f"Mismatch in query {i+1}"

def test_create_table_with_default_values(warehouse):
    table_info = {
        "database": "test_db",
        "schema": "test_schema",
        "table": "test_table",
        "cdc_type": "APPEND_ONLY_STREAM"
    }
    
    source_schema = [
        {
            "name": "id",
            "type": "INTEGER",
            "nullable": False,
            "default_value": "1",
            "primary_key": True
        },
        {
            "name": "status",
            "type": "VARCHAR",
            "nullable": True,
            "default_value": "'ACTIVE'",
            "primary_key": False
        }
    ]
    
    target_schema = source_schema.copy()
    cdc_type = get_cdc_type(table_info)

    with patch.object(warehouse.connection, 'execute') as mock_execute:
        warehouse.create_table(table_info, source_schema, target_schema)
        
        expected_calls = [
            # Create schema
            f"CREATE SCHEMA IF NOT EXISTS {table_info['schema']};",
            
            # Create table with default values
            f"""CREATE OR REPLACE TABLE {table_info['schema']}.{table_info['table']} (id INTEGER NOT NULL, status VARCHAR);""".strip(),
            
            # Insert metadata
            f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.captured_tables VALUES ('test_schema', 'test_table', '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', ['id'], 'APPEND_ONLY_STREAM');""",
            
            f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.source_columns VALUES
            ( 'test_db', 'test_schema', 'test_table', 'id', 'INTEGER', '1', FALSE, TRUE ),
            ( 'test_db', 'test_schema', 'test_table', 'status', 'VARCHAR', '''ACTIVE''', TRUE, FALSE );
            """.strip()
        ]
        
        assert mock_execute.call_count == 4
        actual_calls = [call[0][0].strip() for call in mock_execute.call_args_list]
        for expected, actual in zip(expected_calls, actual_calls):
            assert normalize_sql(expected) == normalize_sql(actual)

# Data Synchronization Tests
def test_sync_table_standard_stream_operations_with_pk(warehouse):
    """Test the complete sync operation including temp table, deletes, inserts and cleanup."""
    table_info = {'schema': 'test_schema', 'table': 'test_table', 'cdc_type': 'STANDARD_STREAM'}
    
    # Create mock batch with required properties
    mock_batch = Mock()
    mock_batch.columns = []
    
    # Create mock DataFrames with batches
    inserts_df = Mock()
    inserts_df.empty = False
    inserts_df.columns = []
    # Create a list with one mock batch and make it iterable
    inserts_df.__iter__ = lambda self: iter([mock_batch])
    
    deletes_df = Mock()
    deletes_df.empty = False
    deletes_df.columns = []
    # Create a list with one mock batch and make it iterable
    deletes_df.__iter__ = lambda self: iter([mock_batch])

    schema_columns = [
        {'name': 'id', 'type': 'INTEGER', 'nullable': False, 'default_value': None, 'primary_key': True},
        {'name': 'name', 'type': 'VARCHAR', 'nullable': True, 'default_value': None, 'primary_key': False},
    ]
    
    with patch.object(warehouse, 'get_schema', return_value=schema_columns), \
         patch.object(warehouse, 'get_primary_keys', return_value=['id']), \
         patch.object(warehouse.connection, 'execute') as mock_execute:
        
        updates_dict = {
            "records_to_insert": inserts_df,
            "records_to_delete": deletes_df
        }

        warehouse.sync_table(table_info, updates_dict)
        
        expected_calls = [
            f"CREATE OR REPLACE TEMP TABLE test_table_deletes_temp AS (SELECT * FROM batch);",
            """DELETE FROM test_schema.test_table WHERE (id) IN ( SELECT (id) FROM test_table_deletes_temp );""".strip(),
            "DROP TABLE IF EXISTS test_table_deletes_temp;",
            """INSERT INTO test_schema.test_table (SELECT id, name FROM processed_batch);""".strip(),
            "UPDATE cdc_schema.captured_tables SET updated_at = current_timestamp WHERE table_name = 'test_table' and schema_name = 'test_schema';"
        ]
        
        # assert mock_execute.call_count == 5
        raw_calls = [args[0] for args, kwargs in mock_execute.call_args_list]
        actual_calls = []
        for call in raw_calls:
            split = call.split('\n')
            new_item = ""
            for line in split:
                new_item += line.strip() + " "
            actual_calls.append(new_item.strip())


        for expected_sql, actual_sql in zip(expected_calls, actual_calls):
            assert expected_sql == actual_sql

def test_sync_table_full_refresh(warehouse):
    """Test the complete sync operation including temp table, deletes, inserts and cleanup."""
    table_info = {'schema': 'test_schema', 'table': 'test_table', 'cdc_type': 'FULL_REFRESH'}

    # Create mock batch with required properties
    mock_batch = Mock()
    mock_batch.columns = []
    
    # Create mock DataFrames with batches
    inserts_df = Mock()
    inserts_df.empty = False
    inserts_df.columns = []
    # Create a list with one mock batch and make it iterable
    inserts_df.__iter__ = lambda self: iter([mock_batch])

    schema_columns = [
        {'name': 'id', 'type': 'INTEGER', 'nullable': False, 'default_value': None, 'primary_key': True},
        {'name': 'name', 'type': 'VARCHAR', 'nullable': True, 'default_value': None, 'primary_key': False},
    ]
    
    with patch.object(warehouse, 'get_schema', return_value=schema_columns), \
         patch.object(warehouse.connection, 'execute') as mock_execute:
        
        updates_dict = {
            "records_to_insert": inserts_df,
            "records_to_delete": None
        }

        warehouse.sync_table(table_info, updates_dict)
        
        expected_calls = [
            "TRUNCATE TABLE test_schema.test_table;",
            "INSERT INTO test_schema.test_table (SELECT id, name FROM processed_batch);"
        ]
        
        actual_calls = [args[0] for args, kwargs in mock_execute.call_args_list]
        for expected_sql, actual_sql in zip(expected_calls, actual_calls):
            assert expected_sql == actual_sql

def test_sync_table_standard_stream_operations_with_two_pk(warehouse):
    """Test the complete sync operation including temp table, deletes, inserts and cleanup."""
    table_info = {'schema': 'test_schema', 'table': 'test_table', 'cdc_type': 'STANDARD_STREAM'}

    # Create mock batch with required properties
    mock_batch = Mock()
    mock_batch.columns = []
    
    # Create mock DataFrames with batches
    inserts_df = Mock()
    inserts_df.empty = False
    inserts_df.columns = []
    # Create a list with one mock batch and make it iterable
    inserts_df.__iter__ = lambda self: iter([mock_batch])
    
    deletes_df = Mock()
    deletes_df.empty = False
    deletes_df.columns = []
    # Create a list with one mock batch and make it iterable
    deletes_df.__iter__ = lambda self: iter([mock_batch])

    schema_columns = [
        {'name': 'id', 'type': 'INTEGER', 'nullable': False, 'default_value': None, 'primary_key': True},
        {'name': 'id2', 'type': 'INTEGER', 'nullable': False, 'default_value': None, 'primary_key': True},
        {'name': 'name', 'type': 'VARCHAR', 'nullable': True, 'default_value': None, 'primary_key': False},
    ]
    
    with patch.object(warehouse, 'get_schema', return_value=schema_columns), \
         patch.object(warehouse, 'get_primary_keys', return_value=['id', 'id2']), \
         patch.object(warehouse.connection, 'execute') as mock_execute:
            
        updates_dict = {
            "records_to_insert": inserts_df,
            "records_to_delete": deletes_df
        }

        warehouse.sync_table(table_info, updates_dict)


        expected_calls = [
            f"CREATE OR REPLACE TEMP TABLE test_table_deletes_temp AS (SELECT * FROM batch);",
            """DELETE FROM test_schema.test_table WHERE (id, id2) IN ( SELECT (id, id2) FROM test_table_deletes_temp );""".strip(),
            "DROP TABLE IF EXISTS test_table_deletes_temp;",
            """INSERT INTO test_schema.test_table (SELECT id, id2, name FROM processed_batch);""".strip(),
            "UPDATE cdc_schema.captured_tables SET updated_at = current_timestamp WHERE table_name = 'test_table' and schema_name = 'test_schema';"
        ]
        
        # assert mock_execute.call_count == 5
        raw_calls = [args[0] for args, kwargs in mock_execute.call_args_list]
        actual_calls = []
        for call in raw_calls:
            split = call.split('\n')
            new_item = ""
            for line in split:
                new_item += line.strip() + " "
            actual_calls.append(new_item.strip())

        for expected_sql, actual_sql in zip(expected_calls, actual_calls):
            assert expected_sql == actual_sql


def test_sync_table_append_only_stream(warehouse):
    """Test the complete sync operation including temp table, deletes, inserts and cleanup."""
    table_info = {'schema': 'test_schema', 'table': 'test_table', 'cdc_type': 'APPEND_ONLY_STREAM'}

    # Create mock batch with required properties
    mock_batch = Mock()
    mock_batch.columns = []
    
    # Create mock DataFrames with batches
    inserts_df = Mock()
    inserts_df.empty = False
    inserts_df.columns = []
    # Create a list with one mock batch and make it iterable
    inserts_df.__iter__ = lambda self: iter([mock_batch])
    
    schema_columns = [
        {'name': 'name', 'type': 'VARCHAR', 'nullable': True, 'default_value': None, 'primary_key': False},
        {'name': 'value', 'type': 'INTEGER', 'nullable': True, 'default_value': None, 'primary_key': False},
    ]
    
    with patch.object(warehouse, 'get_schema', return_value=schema_columns), \
         patch.object(warehouse, 'get_primary_keys', return_value=['melchi_row_id']), \
         patch.object(warehouse.connection, 'execute') as mock_execute:
            
        updates_dict = {
            "records_to_insert": inserts_df,
            "records_to_delete": None
        }
        warehouse.sync_table(table_info, updates_dict)


        expected_calls = [
            """INSERT INTO test_schema.test_table (SELECT name, value FROM processed_batch);""".strip(),
            "UPDATE cdc_schema.captured_tables SET updated_at = current_timestamp WHERE table_name = 'test_table' and schema_name = 'test_schema';"
        ]
        
        # assert mock_execute.call_count == 5
        raw_calls = [args[0] for args, kwargs in mock_execute.call_args_list]
        actual_calls = []
        for call in raw_calls:
            split = call.split('\n')
            new_item = ""
            for line in split:
                new_item += line.strip() + " "
            actual_calls.append(new_item.strip())

        for expected_sql, actual_sql in zip(expected_calls, actual_calls):
            assert expected_sql == actual_sql

def test_sync_table_no_rows(warehouse):
    """Test sync operation when table has no primary keys."""
    standard_stream_table_info = {'schema': 'test_schema', 'table': 'standard_stream_table', 'cdc_type': 'STANDARD_STREAM'}
    append_only_stream_table_info = {'schema': 'test_schema', 'table': 'append_only_stream_table', 'cdc_type': 'APPEND_ONLY_STREAM'}
    full_refresh_table_info = {'schema': 'test_schema', 'table': 'full_refresh_table', 'cdc_type': 'FULL_REFRESH'}

    mock_batch = Mock()
    mock_batch.columns = []

    # Create mock DataFrames with batches
    insert_batches = Mock()
    # Create a list with one mock batch and make it iterable
    insert_batches.__iter__ = lambda self: iter([mock_batch])
    
    delete_batches = Mock()
    # Create a list with one mock batch and make it iterable
    delete_batches.__iter__ = lambda self: iter([mock_batch])


    updates_dict_with_all = {
        "records_to_insert": [],
        "records_to_delete": []
    }

    updates_dict_with_no_deletes = {
        "records_to_insert": [],
        "records_to_delete": None
    }

    with patch.object(warehouse.connection, 'execute') as mock_execute:

        warehouse.sync_table(standard_stream_table_info, updates_dict_with_all)
        warehouse.sync_table(append_only_stream_table_info, updates_dict_with_no_deletes)
        warehouse.sync_table(full_refresh_table_info, updates_dict_with_no_deletes)
            
        actual_calls = [args[0] for args, kwargs in mock_execute.call_args_list]
        expected_calls = [
            "UPDATE cdc_schema.captured_tables SET updated_at = current_timestamp WHERE table_name = 'standard_stream_table' and schema_name = 'test_schema';",
            "UPDATE cdc_schema.captured_tables SET updated_at = current_timestamp WHERE table_name = 'append_only_stream_table' and schema_name = 'test_schema';",
            "TRUNCATE TABLE test_schema.full_refresh_table;",
            "UPDATE cdc_schema.captured_tables SET updated_at = current_timestamp WHERE table_name = 'full_refresh_table' and schema_name = 'test_schema';"
        ]
        print("")

        for expected_sql, actual_sql in zip(expected_calls, actual_calls):
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

