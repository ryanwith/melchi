import pytest
from datetime import datetime
from unittest.mock import Mock, patch, call
from src.warehouses.duckdb_warehouse import DuckDBWarehouse
from tests.utils.helpers import normalize_sql
from pprint import pp
from tests.utils.helpers import normalize_sql

@pytest.fixture
def config():
    return {
        'database': 'test_db.duckdb',
        'change_tracking_schema': 'cdc_schema',
        'warehouse_role': 'TARGET',
        'replace_existing': False
    }

@pytest.fixture
def warehouse(config):
    return DuckDBWarehouse(config)

class TestConnectionManagement:
    def test_connect_basic(self, warehouse):
        """Test basic connection establishment"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            
            # Verify connection was attempted with correct database
            mock_connect.assert_called_once_with(warehouse.config['database'])
            
            # Verify spatial extension setup
            mock_connection.execute.assert_has_calls([
                call("INSTALL spatial;"),
                call("LOAD spatial;")
            ])
            
            assert warehouse.connection == mock_connection

    def test_connect_invalid_database_path(self, warehouse):
        """Test error handling for invalid database path"""
        with patch('duckdb.connect', side_effect=Exception("Unable to open database file")):
            with pytest.raises(Exception) as exc_info:
                warehouse.connect()
            assert "Unable to open database file" in str(exc_info.value)
            assert warehouse.connection is None

    def test_disconnect_when_connected(self, warehouse):
        """Test proper disconnection when connected"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            warehouse.disconnect()
            
            mock_connection.close.assert_called_once()
            assert warehouse.connection is None

    def test_disconnect_when_not_connected(self, warehouse):
        """Test disconnection when not connected"""
        warehouse.connection = None
        warehouse.disconnect()  # Should not raise any errors

    def test_reconnect_after_disconnect(self, warehouse):
        """Test connecting after a previous disconnect"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            # First connection
            warehouse.connect()
            warehouse.disconnect()
            
            # Second connection
            warehouse.connect()
            
            assert mock_connect.call_count == 2
            assert warehouse.connection is not None

class TestTransactionManagement:
    def test_begin_transaction(self, warehouse):
        """Test beginning a transaction"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            warehouse.begin_transaction()
            
            mock_connection.begin.assert_called_once()

    def test_commit_transaction(self, warehouse):
        """Test committing a transaction"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            warehouse.begin_transaction()
            warehouse.commit_transaction()
            
            mock_connection.begin.assert_called_once()
            mock_connection.commit.assert_called_once()

    def test_rollback_transaction(self, warehouse):
        """Test rolling back a transaction"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            warehouse.begin_transaction()
            warehouse.rollback_transaction()
            
            mock_connection.begin.assert_called_once()
            mock_connection.rollback.assert_called_once()

    def test_multiple_transactions(self, warehouse):
        """Test multiple begin/commit cycles"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            
            # First transaction
            warehouse.begin_transaction()
            warehouse.commit_transaction()
            
            # Second transaction
            warehouse.begin_transaction()
            warehouse.commit_transaction()
            
            assert mock_connection.begin.call_count == 2
            assert mock_connection.commit.call_count == 2

    def test_transaction_after_reconnect(self, warehouse):
        """Test transaction behavior after reconnecting"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            # First connection and transaction
            warehouse.connect()
            warehouse.begin_transaction()
            warehouse.commit_transaction()
            warehouse.disconnect()
            
            # Second connection and transaction
            warehouse.connect()
            warehouse.begin_transaction()
            warehouse.commit_transaction()
            
            assert mock_connect.call_count == 2
            assert mock_connection.begin.call_count == 2
            assert mock_connection.commit.call_count == 2

    def test_transaction_error_handling(self, warehouse):
        """Test transaction error handling"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connection.commit.side_effect = Exception("Transaction error")
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            warehouse.begin_transaction()
            
            with pytest.raises(Exception) as exc_info:
                warehouse.commit_transaction()
            
            assert "Transaction error" in str(exc_info.value)
            mock_connection.begin.assert_called_once()
            mock_connection.commit.assert_called_once()

    def test_nested_transaction_behavior(self, warehouse):
        """Test behavior with nested transactions (if supported by DuckDB)"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            
            # First (outer) transaction
            warehouse.begin_transaction()
            
            # Second (inner) transaction
            warehouse.begin_transaction()
            
            # Commit both
            warehouse.commit_transaction()
            warehouse.commit_transaction()
            
            # Verify calls were made in correct order
            assert mock_connection.begin.call_count == 2
            assert mock_connection.commit.call_count == 2

class TestSchemaAndMetadata:
    def test_get_schema_basic(self, warehouse):
            """Test basic schema retrieval"""
            with patch('duckdb.connect') as mock_connect:
                mock_connection = Mock()
                mock_result = Mock()
                
                # Make fetchall return a list that can be iterated
                mock_schema_rows = [
                    (0, "id", "INTEGER", "FALSE", None, "TRUE"),
                    (1, "name", "VARCHAR", "TRUE", None, "FALSE"),
                    (2, "created_at", "TIMESTAMP", "FALSE", "CURRENT_TIMESTAMP", "FALSE")
                ]
                mock_result.fetchall.return_value = mock_schema_rows
                
                # Make execute return our mock_result
                mock_connection.execute.return_value = mock_result
                mock_connect.return_value = mock_connection
                
                table_info = {
                    "schema": "test_schema",
                    "table": "test_table"
                }
                
                warehouse.connect()
                schema = warehouse.get_schema(table_info)
                
                # Verify PRAGMA call
                expected_query = f"PRAGMA table_info('test_schema.test_table');"
                mock_connection.execute.assert_called_with(expected_query)
                
                # Verify schema format
                assert len(schema) == 3
                assert schema[0] == {
                    "name": "id",
                    "type": "INTEGER",
                    "nullable": False,
                    "default_value": None,
                    "primary_key": True
                }
                assert schema[1] == {
                    "name": "name",
                    "type": "VARCHAR",
                    "nullable": True,
                    "default_value": None,
                    "primary_key": False
                }
                assert schema[2] == {
                    "name": "created_at",
                    "type": "TIMESTAMP",
                    "nullable": False,
                    "default_value": "CURRENT_TIMESTAMP",
                    "primary_key": False
                }

    def test_create_table_standard_stream_with_pk(self, warehouse):
        """Test creating a table for standard stream with primary keys"""
        with patch('duckdb.connect') as mock_connect:
            # Setup mock connection
            mock_connection = Mock()
            
            # Configure table_exists check to return False
            mock_exists_result = Mock()
            mock_exists_result.fetchone.return_value = None
            
            # Make execute return our mock_exists_result for table existence check
            def mock_execute_return(sql):
                if "information_schema.tables" in sql:
                    return mock_exists_result
                return Mock()

            mock_connection.execute.side_effect = mock_execute_return
            mock_connect.return_value = mock_connection
            warehouse.config["replace_existing"] = True
      
            warehouse.connect()
            
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "STANDARD_STREAM"
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
            
            target_schema = source_schema.copy()
            
            with patch('datetime.datetime') as mock_datetime:
                mock_datetime.now.return_value = datetime(2024, 1, 1, 12, 0, 0)
                warehouse.create_table(table_info, source_schema, target_schema)
                
                expected_calls = [
                    # From connect()
                    call("INSTALL spatial;"),
                    call("LOAD spatial;"),
                                        
                    # From create_table()
                    call(f"CREATE SCHEMA IF NOT EXISTS {table_info['schema']};"),
                    call(f"CREATE OR REPLACE TABLE {table_info['schema']}.{table_info['table']} (id INTEGER NOT NULL, name VARCHAR);"),
                    call(f"""DELETE FROM {warehouse.get_change_tracking_schema_full_name()}.captured_tables 
                         WHERE schema_name = '{table_info['schema']}' and table_name = '{table_info['table']}';"""),
                    call(f"""DELETE FROM {warehouse.get_change_tracking_schema_full_name()}.source_columns 
                         WHERE table_schema = '{table_info['schema']}' and table_name = '{table_info['table']}';"""),
                    call(f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.captured_tables 
                         VALUES ('{table_info['schema']}', '{table_info['table']}', '2024-01-01 12:00:00', '2024-01-01 12:00:00', ['id'], 'STANDARD_STREAM');"""),
                    call(f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.source_columns 
                         VALUES ('test_db', 'test_schema', 'test_table', 'id', 'INTEGER', NULL, FALSE, TRUE),
                               ('test_db', 'test_schema', 'test_table', 'name', 'VARCHAR', NULL, TRUE, FALSE);""")
                ]
                
                assert mock_connection.execute.call_count == len(expected_calls)
                for expected, actual in zip(expected_calls, mock_connection.execute.call_args_list):
                    assert normalize_sql(expected.args[0]) == normalize_sql(actual.args[0])


    def test_create_table_standard_stream_no_pk(self, warehouse):
        """Test creating a table for standard stream without primary keys"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection

            warehouse.config["replace_existing"] = True
            warehouse.connect()
            
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "STANDARD_STREAM"
            }
            
            source_schema = [
                {
                    "name": "name",
                    "type": "VARCHAR",
                    "nullable": True,
                    "default_value": None,
                    "primary_key": False
                },
                {
                    "name": "value",
                    "type": "INTEGER",
                    "nullable": True,
                    "default_value": None,
                    "primary_key": False
                }
            ]
            
            target_schema = source_schema.copy()
            
            with patch('datetime.datetime') as mock_datetime:
                mock_datetime.now.return_value = datetime(2024, 1, 1, 12, 0, 0)
                warehouse.create_table(table_info, source_schema, target_schema)
                
                # Should automatically add MELCHI_ROW_ID column for standard streams without PKs
                expected_calls = [
                    call("INSTALL spatial;"),
                    call("LOAD spatial;"),
                    call(f"CREATE SCHEMA IF NOT EXISTS {table_info['schema']};"),
                    call(f"CREATE OR REPLACE TABLE {table_info['schema']}.{table_info['table']} (name VARCHAR, value INTEGER, MELCHI_ROW_ID VARCHAR NOT NULL);"),
                    call(f"""DELETE FROM {warehouse.get_change_tracking_schema_full_name()}.captured_tables 
                         WHERE schema_name = '{table_info['schema']}' and table_name = '{table_info['table']}';"""),
                    call(f"""DELETE FROM {warehouse.get_change_tracking_schema_full_name()}.source_columns 
                         WHERE table_schema = '{table_info['schema']}' and table_name = '{table_info['table']}';"""),
                    call(f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.captured_tables 
                         VALUES ('{table_info['schema']}', '{table_info['table']}', '2024-01-01 12:00:00', '2024-01-01 12:00:00', ['MELCHI_ROW_ID'], 'STANDARD_STREAM');"""),
                    call(f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.source_columns 
                         VALUES ('test_db', 'test_schema', 'test_table', 'name', 'VARCHAR', NULL, TRUE, FALSE),
                               ('test_db', 'test_schema', 'test_table', 'value', 'INTEGER', NULL, TRUE, FALSE);""")
                ]
                
                assert mock_connection.execute.call_count == len(expected_calls)
                for expected, actual in zip(expected_calls, mock_connection.execute.call_args_list):
                    assert normalize_sql(expected.args[0]) == normalize_sql(actual.args[0])

    def test_create_table_append_only_stream(self, warehouse):
        """Test creating a table for append-only stream"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            warehouse.config["replace_existing"] = True
            warehouse.connect()
            
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
                    "default_value": None,
                    "primary_key": True
                }
            ]
            
            target_schema = source_schema.copy()
            
            with patch('datetime.datetime') as mock_datetime:
                mock_datetime.now.return_value = datetime(2024, 1, 1, 12, 0, 0)
                warehouse.create_table(table_info, source_schema, target_schema)
                
                # Should not add MELCHI_ROW_ID for append-only streams
                expected_calls = [
                    call("INSTALL spatial;"),
                    call("LOAD spatial;"),
                    call(f"CREATE SCHEMA IF NOT EXISTS {table_info['schema']};"),
                    call(f"CREATE OR REPLACE TABLE {table_info['schema']}.{table_info['table']} (id INTEGER NOT NULL);"),
                    call(f"""DELETE FROM {warehouse.get_change_tracking_schema_full_name()}.captured_tables 
                         WHERE schema_name = '{table_info['schema']}' and table_name = '{table_info['table']}';"""),
                    call(f"""DELETE FROM {warehouse.get_change_tracking_schema_full_name()}.source_columns 
                         WHERE table_schema = '{table_info['schema']}' and table_name = '{table_info['table']}';"""),
                    call(f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.captured_tables 
                         VALUES ('{table_info['schema']}', '{table_info['table']}', '2024-01-01 12:00:00', '2024-01-01 12:00:00', ['id'], 'APPEND_ONLY_STREAM');"""),
                    call(f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.source_columns 
                         VALUES ('test_db', 'test_schema', 'test_table', 'id', 'INTEGER', NULL, FALSE, TRUE);""")
                ]
                
                # assert mock_connection.execute.call_count == len(expected_calls)
                # for actual in mock_connection.execute.call_args_list:
                #     print(actual)
                for expected, actual in zip(expected_calls, mock_connection.execute.call_args_list):
                    assert normalize_sql(expected.args[0]) == normalize_sql(actual.args[0])

    def test_create_table_with_default_values(self, warehouse):
        """Test creating a table with default values"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            warehouse.config["replace_existing"] = True
            warehouse.connect()
            
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "FULL_REFRESH"
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
            
            with patch('datetime.datetime') as mock_datetime:
                mock_datetime.now.return_value = datetime(2024, 1, 1, 12, 0, 0)
                warehouse.create_table(table_info, source_schema, target_schema)
                
                expected_calls = [
                    call("INSTALL spatial;"),
                    call("LOAD spatial;"),
                    call(f"CREATE SCHEMA IF NOT EXISTS {table_info['schema']};"),
                    call(f"CREATE OR REPLACE TABLE {table_info['schema']}.{table_info['table']} (id INTEGER NOT NULL, status VARCHAR);"),
                    call(f"""DELETE FROM {warehouse.get_change_tracking_schema_full_name()}.captured_tables 
                         WHERE schema_name = '{table_info['schema']}' and table_name = '{table_info['table']}';"""),
                    call(f"""DELETE FROM {warehouse.get_change_tracking_schema_full_name()}.source_columns 
                         WHERE table_schema = '{table_info['schema']}' and table_name = '{table_info['table']}';"""),
                    call(f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.captured_tables 
                         VALUES ('{table_info['schema']}', '{table_info['table']}', '2024-01-01 12:00:00', '2024-01-01 12:00:00', ['id'], 'FULL_REFRESH');"""),
                    call(f"""INSERT INTO {warehouse.get_change_tracking_schema_full_name()}.source_columns 
                         VALUES ('test_db', 'test_schema', 'test_table', 'id', 'INTEGER', '1', FALSE, TRUE),
                               ('test_db', 'test_schema', 'test_table', 'status', 'VARCHAR', '''ACTIVE''', TRUE, FALSE);""")
                ]
                
                assert mock_connection.execute.call_count == len(expected_calls)
                for expected, actual in zip(expected_calls, mock_connection.execute.call_args_list):
                    assert normalize_sql(expected.args[0]) == normalize_sql(actual.args[0])

    def test_create_table_all_data_types(self, warehouse):
        """Test creating a table with all supported DuckDB data types"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            warehouse.config["replace_existing"] = True
            warehouse.connect()
            
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "FULL_REFRESH"
            }
            
            source_schema = [
                {"name": "int_col", "type": "INTEGER", "nullable": False, "default_value": None, "primary_key": True},
                {"name": "bigint_col", "type": "BIGINT", "nullable": True, "default_value": None, "primary_key": False},
                {"name": "decimal_col", "type": "DECIMAL(10,2)", "nullable": True, "default_value": None, "primary_key": False},
                {"name": "varchar_col", "type": "VARCHAR", "nullable": True, "default_value": None, "primary_key": False},
                {"name": "boolean_col", "type": "BOOLEAN", "nullable": True, "default_value": None, "primary_key": False},
                {"name": "timestamp_col", "type": "TIMESTAMP", "nullable": True, "default_value": None, "primary_key": False},
                {"name": "date_col", "type": "DATE", "nullable": True, "default_value": None, "primary_key": False},
                {"name": "time_col", "type": "TIME", "nullable": True, "default_value": None, "primary_key": False},
                {"name": "blob_col", "type": "BLOB", "nullable": True, "default_value": None, "primary_key": False},
                {"name": "json_col", "type": "JSON", "nullable": True, "default_value": None, "primary_key": False},
                {"name": "geometry_col", "type": "GEOMETRY", "nullable": True, "default_value": None, "primary_key": False}
            ]
            
            target_schema = source_schema.copy()
            
            with patch('datetime.datetime') as mock_datetime:
                mock_datetime.now.return_value = datetime(2024, 1, 1, 12, 0, 0)
                warehouse.create_table(table_info, source_schema, target_schema)

                
                expected_calls = [
                    "INSTALL spatial;",
                    "LOAD spatial;",
                    "CREATE SCHEMA IF NOT EXISTS test_schema;",
                    "CREATE OR REPLACE TABLE test_schema.test_table (int_col INTEGER NOT NULL, bigint_col BIGINT, decimal_col DECIMAL(10,2), varchar_col VARCHAR, boolean_col BOOLEAN, timestamp_col TIMESTAMP, date_col DATE, time_col TIME, blob_col BLOB, json_col JSON, geometry_col GEOMETRY);",
                    "DELETE FROM cdc_schema.captured_tables WHERE schema_name = 'test_schema' and table_name = 'test_table';",
                    "DELETE FROM cdc_schema.source_columns WHERE table_schema = 'test_schema' and table_name = 'test_table';",
                    "INSERT INTO cdc_schema.captured_tables VALUES ('test_schema', 'test_table', '2024-01-01 12:00:00', '2024-01-01 12:00:00', ['int_col'], 'FULL_REFRESH');",
                    "INSERT INTO cdc_schema.source_columns VALUES ('test_db', 'test_schema', 'test_table', 'int_col', 'INTEGER', NULL, FALSE, TRUE), ('test_db', 'test_schema', 'test_table', 'bigint_col', 'BIGINT', NULL, TRUE, FALSE), ('test_db', 'test_schema', 'test_table', 'decimal_col', 'DECIMAL(10,2)', NULL, TRUE, FALSE), ('test_db', 'test_schema', 'test_table', 'varchar_col', 'VARCHAR', NULL, TRUE, FALSE), ('test_db', 'test_schema', 'test_table', 'boolean_col', 'BOOLEAN', NULL, TRUE, FALSE), ('test_db', 'test_schema', 'test_table', 'timestamp_col', 'TIMESTAMP', NULL, TRUE, FALSE), ('test_db', 'test_schema', 'test_table', 'date_col', 'DATE', NULL, TRUE, FALSE), ('test_db', 'test_schema', 'test_table', 'time_col', 'TIME', NULL, TRUE, FALSE), ('test_db', 'test_schema', 'test_table', 'blob_col', 'BLOB', NULL, TRUE, FALSE), ('test_db', 'test_schema', 'test_table', 'json_col', 'JSON', NULL, TRUE, FALSE), ('test_db', 'test_schema', 'test_table', 'geometry_col', 'GEOMETRY', NULL, TRUE, FALSE);",
                    f"""CREATE OR REPLACE TABLE {table_info['schema']}.{table_info['table']} (
                        int_col INTEGER NOT NULL,
                        bigint_col BIGINT,
                        decimal_col DECIMAL(10,2),
                        varchar_col VARCHAR,
                        boolean_col BOOLEAN,
                        timestamp_col TIMESTAMP,
                        date_col DATE,
                        time_col TIME,
                        blob_col BLOB,
                        json_col JSON,
                        geometry_col GEOMETRY
                        );"""
                    ]
                
                for expected, actual in zip(expected_calls, mock_connection.execute.call_args_list):
                    assert normalize_sql(expected) == normalize_sql(actual.args[0])
                

    def test_setup_cdc_tracking_tables(self, warehouse):
        """Test creation of CDC tracking tables"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            warehouse.connect()
            
            warehouse._setup_target_environment()
            
            expected_calls = [
                "INSTALL spatial;",
                "LOAD spatial;",
                f"CREATE SCHEMA IF NOT EXISTS {warehouse.get_change_tracking_schema_full_name()};",
                f"""CREATE TABLE IF NOT EXISTS {warehouse.get_change_tracking_schema_full_name()}.captured_tables 
                     (schema_name varchar, table_name varchar, created_at timestamp, updated_at timestamp, 
                     primary_keys varchar[], cdc_type varchar);""",
                f"""CREATE TABLE IF NOT EXISTS {warehouse.get_change_tracking_schema_full_name()}.source_columns 
                     (table_catalog varchar, table_schema varchar, table_name varchar, column_name varchar, 
                     data_type varchar, column_default varchar, is_nullable boolean, primary_key boolean);""",
                f"""CREATE TABLE IF NOT EXISTS {warehouse.get_change_tracking_schema_full_name()}.etl_events 
                     (schema_name varchar, table_name varchar, etl_id varchar, completed_at timestamp_ns default current_timestamp);"""
            ]
            
            assert mock_connection.execute.call_count == len(expected_calls)
            for expected, actual in zip(expected_calls, mock_connection.execute.call_args_list):
                assert normalize_sql(expected) == normalize_sql(actual.args[0])

    def test_table_exists(self, warehouse):
        """Test table existence check"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            # Mock exists response
            mock_connection.execute().fetchone.return_value = ['test_table']
            
            warehouse.connect()
            table_info = {
                "schema": "test_schema",
                "table": "test_table"
            }
            
            assert warehouse.table_exists(table_info) == True
            
            expected_query = """
                SELECT * FROM information_schema.tables 
                WHERE table_schema = 'test_schema' AND table_name = 'test_table';
            """
            
            actual_query = mock_connection.execute.call_args[0][0]
            assert normalize_sql(actual_query) == normalize_sql(expected_query)

    def test_get_primary_keys(self, warehouse):
        """Test retrieval of primary keys"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            mock_connection.execute().fetchone.return_value = [['id', 'email']]
            
            warehouse.connect()
            table_info = {
                "schema": "test_schema",
                "table": "test_table"
            }
            
            primary_keys = warehouse.get_primary_keys(table_info)
            assert primary_keys == ['email', 'id']
            
            expected_query = f"""
                SELECT primary_keys FROM {warehouse.get_change_tracking_schema_full_name()}.captured_tables
                WHERE table_name = 'test_table' and schema_name = 'test_schema'
            """
            
            actual_query = mock_connection.execute.call_args[0][0]
            assert normalize_sql(actual_query) == normalize_sql(expected_query)

    def test_spatial_extension_loading(self, warehouse):
        """Test spatial extension loading during connection"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            
            mock_connection.execute.assert_has_calls([
                call("INSTALL spatial;"),
                call("LOAD spatial;")
            ])

class TestCDCOperations:
    def test_full_refresh_basic(self, warehouse):
        """Test basic full refresh operation"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_result = Mock()
            
            # Mock schema result
            mock_schema_rows = [
                (0, "id", "INTEGER", "FALSE", None, "TRUE"),
                (1, "name", "VARCHAR", "TRUE", None, "FALSE")
            ]
            mock_result.fetchall.return_value = mock_schema_rows
            mock_connection.execute.return_value = mock_result
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "FULL_REFRESH"
            }
            
            etl_id = "test-etl-id"
            
            # Create a mock DataFrame with the expected columns
            mock_df = Mock()
            mock_df.columns = ['id', 'name']
            df_batches = [mock_df]
            
            # Execute full refresh operation
            warehouse.truncate_table(table_info)
            warehouse.process_insert_batches(table_info, df_batches, lambda x: x)
            warehouse.update_cdc_trackers(table_info, etl_id)
            
            expected_calls = [
                # From connect()
                "INSTALL spatial;",
                "LOAD spatial;",

                
                # From truncate_table()
                "TRUNCATE TABLE test_schema.test_table;",
                
                # From get_schema() in process_insert_batches()
                "PRAGMA table_info('test_schema.test_table');",

                # From process_insert_batches()
                "INSERT INTO test_schema.test_table (SELECT id, name FROM processed_df);",
                
                # From update_cdc_trackers()
                "UPDATE cdc_schema.captured_tables SET updated_at = current_timestamp WHERE table_name = 'test_table' AND schema_name = 'test_schema';",
                f"INSERT INTO cdc_schema.etl_events VALUES ('test_schema', 'test_table', '{etl_id}', current_timestamp::timestamp);"
            ]
            
            assert mock_connection.execute.call_count == len(expected_calls)
            print("")
            for expected, actual in zip(expected_calls, mock_connection.execute.call_args_list):
                assert normalize_sql(expected) == normalize_sql(actual.args[0])

    def test_standard_stream_basic(self, warehouse):
        """Test basic standard stream operation"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_result = Mock()
            
            # Mock schema result
            mock_schema_rows = [
                (0, "id", "INTEGER", "FALSE", None, "TRUE"),
                (1, "name", "VARCHAR", "TRUE", None, "FALSE"),
                (2, "MELCHI_ROW_ID", "VARCHAR", "FALSE", None, "TRUE")
            ]
            
            # Create a mock for the primary keys query result
            mock_pk_result = Mock()
            mock_pk_result.fetchone.return_value = [['MELCHI_ROW_ID']]
            
            # Configure execute to return different results based on the query
            def mock_execute(query):
                if "SELECT primary_keys FROM" in query:
                    return mock_pk_result
                return mock_result
                
            mock_result.fetchall.return_value = mock_schema_rows
            mock_connection.execute = Mock(side_effect=mock_execute)
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "STANDARD_STREAM"
            }
            
            etl_id = "test-etl-id"
            
            # Create mock DataFrames with expected columns
            mock_delete_df = Mock()
            mock_delete_df.columns = ['MELCHI_ROW_ID']
            delete_batches = [mock_delete_df]
            
            mock_insert_df = Mock()
            mock_insert_df.columns = ['id', 'name', 'MELCHI_ROW_ID']
            insert_batches = [mock_insert_df]
            
            # Execute standard stream operations
            warehouse.process_delete_batches(table_info, delete_batches, lambda x: x)
            warehouse.process_insert_batches(table_info, insert_batches, lambda x: x)
            warehouse.update_cdc_trackers(table_info, etl_id)
            
            expected_calls = [
                # From connect()
                "INSTALL spatial;",
                "LOAD spatial;",
                
                # From get_primary_keys() in process_delete_batches()
                f"""SELECT primary_keys FROM cdc_schema.captured_tables
                    WHERE table_name = 'test_table' and schema_name = 'test_schema'""",
                
                # From process_delete_batches()
                "DELETE FROM test_schema.test_table WHERE (MELCHI_ROW_ID) IN ( SELECT (MELCHI_ROW_ID) FROM processed_df );",
                
                # From get_schema() in process_insert_batches()
                "PRAGMA table_info('test_schema.test_table');",
                
                # From process_insert_batches()
                "INSERT INTO test_schema.test_table (SELECT id, name, MELCHI_ROW_ID FROM processed_df);",
                
                # From update_cdc_trackers()
                "UPDATE cdc_schema.captured_tables SET updated_at = current_timestamp WHERE table_name = 'test_table' AND schema_name = 'test_schema';",
                f"INSERT INTO cdc_schema.etl_events VALUES ('test_schema', 'test_table', '{etl_id}', current_timestamp::timestamp);"
            ]
            # assert mock_connection.execute.call_count == len(expected_calls)
            for expected, actual in zip(expected_calls, mock_connection.execute.call_args_list):
                assert normalize_sql(expected) == normalize_sql(actual.args[0])
            # for actual in mock_connection.execute.call_args_list:
                # print(normalize_sql(expected))
                # print(normalize_sql(actual.args[0]))
                # assert normalize_sql(expected.args[0]) == normalize_sql(actual.args[0])

    def test_append_only_stream_basic(self, warehouse):
        """Test basic append-only stream operation"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_result = Mock()
            
            # Mock schema result
            mock_schema_rows = [
                (0, "id", "INTEGER", "FALSE", None, "TRUE"),
                (1, "name", "VARCHAR", "TRUE", None, "FALSE")
            ]
            mock_result.fetchall.return_value = mock_schema_rows
            mock_connection.execute.return_value = mock_result
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "APPEND_ONLY_STREAM"
            }
            
            etl_id = "test-etl-id"
            
            # Create a mock DataFrame with the expected columns
            mock_df = Mock()
            mock_df.columns = ['id', 'name']
            df_batches = [mock_df]
            
            # Execute append-only stream operations
            warehouse.process_insert_batches(table_info, df_batches, lambda x: x)
            warehouse.update_cdc_trackers(table_info, etl_id)
            
            expected_calls = [
                # From connect()
                "INSTALL spatial;",
                "LOAD spatial;",
                
                # From get_schema() in process_insert_batches()
                "PRAGMA table_info('test_schema.test_table');",
                
                # From process_insert_batches()
                "INSERT INTO test_schema.test_table (SELECT id, name FROM processed_df);",
                
                # From update_cdc_trackers()
                "UPDATE cdc_schema.captured_tables SET updated_at = current_timestamp WHERE table_name = 'test_table' AND schema_name = 'test_schema';",
                f"INSERT INTO cdc_schema.etl_events VALUES ('test_schema', 'test_table', '{etl_id}', current_timestamp::timestamp);"
            ]
            
            assert mock_connection.execute.call_count == len(expected_calls)
            for expected, actual in zip(expected_calls, mock_connection.execute.call_args_list):
                assert normalize_sql(expected) == normalize_sql(actual.args[0])

    def test_cdc_tracking_table_updates(self, warehouse):
        """Test CDC tracking table update operations"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_result = Mock()
            mock_result.fetchall.return_value = []
            mock_connection.execute = Mock(return_value=mock_result)
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table"
            }
            etl_id = "test-etl-id"
            
            warehouse.update_cdc_trackers(table_info, etl_id)
            
            expected_calls = [
                # From connect()
                "INSTALL spatial;",
                "LOAD spatial;",
                
                # From update_cdc_trackers()
                "UPDATE cdc_schema.captured_tables SET updated_at = current_timestamp WHERE table_name = 'test_table' AND schema_name = 'test_schema';",
                f"INSERT INTO cdc_schema.etl_events VALUES ('test_schema', 'test_table', '{etl_id}', current_timestamp::timestamp);"
            ]
            
            assert mock_connection.execute.call_count == len(expected_calls)
            for expected, actual in zip(expected_calls, mock_connection.execute.call_args_list):
                assert normalize_sql(expected) == normalize_sql(actual.args[0])

    def test_etl_id_management(self, warehouse):
        """Test ETL ID retrieval and management"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_result = Mock()
            
            # Mock execute_query to return properly formatted ETL IDs
            mock_connection.execute = Mock(return_value=mock_result)
            mock_connect.return_value = mock_connection
            
            # Setup execute_query to return the mock ETL IDs
            with patch.object(warehouse, 'execute_query', return_value=[
                ("old-etl-1",),
                ("old-etl-2",)
            ]):
                warehouse.connect()
                table_info = {
                    "database": "test_db",
                    "schema": "test_schema",
                    "table": "test_table"
                }
                
                etl_ids = warehouse.get_etl_ids(table_info)
                
                expected_calls = [
                    # From connect()
                    "INSTALL spatial;",
                    "LOAD spatial;"
                ]
                
                assert etl_ids == ["old-etl-1", "old-etl-2"]
                assert mock_connection.execute.call_count == len(expected_calls)
                for expected, actual in zip(expected_calls, mock_connection.execute.call_args_list):
                    assert normalize_sql(expected) == normalize_sql(actual.args[0])

    def test_cleanup_operations(self, warehouse):
        """Test cleanup operations for tables"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_result = Mock()
            mock_result.fetchall.return_value = []
            mock_connection.execute = Mock(return_value=mock_result)
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table"
            }
            
            warehouse.truncate_table(table_info)
            
            expected_calls = [
                # From connect()
                "INSTALL spatial;",
                "LOAD spatial;",
                
                # From truncate_table()
                "TRUNCATE TABLE test_schema.test_table;"
            ]
            
            assert mock_connection.execute.call_count == len(expected_calls)
            for expected, actual in zip(expected_calls, mock_connection.execute.call_args_list):
                assert normalize_sql(expected) == normalize_sql(actual.args[0])

class TestErrorHandling:
    def test_invalid_table_names(self, warehouse):
        """Test handling of operations with invalid table names"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            # Setup the mock to allow connection but fail on table operations
            def mock_execute(sql):
                if 'INSTALL' in sql or 'LOAD' in sql:
                    return Mock()
                raise Exception("Invalid table name: Table does not exist")
            
            mock_connection.execute.side_effect = mock_execute
            
            warehouse.connect()
            table_info = {
                "schema": "test_schema",
                "table": "invalid_table$name"  # Invalid table name with special character
            }
            
            # Test schema retrieval
            with pytest.raises(Exception) as exc_info:
                warehouse.get_schema(table_info)
            assert "Invalid table name" in str(exc_info.value)

    def test_missing_permissions(self, warehouse):
        """Test behavior when operations fail due to missing permissions"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            # Setup mock result for table existence check
            mock_exists_result = Mock()
            mock_exists_result.fetchone.return_value = None  # Table doesn't exist
            
            # Setup mock to allow connection but fail on schema/table creation
            def mock_execute(sql):
                if 'INSTALL' in sql or 'LOAD' in sql:
                    return Mock()
                elif "information_schema.tables" in sql:
                    return mock_exists_result
                elif "CREATE SCHEMA" in sql:
                    raise Exception("Permission denied: Unable to create schema")
                elif "CREATE" in sql:  # Changed from "CREATE TABLE" to catch all CREATE statements
                    raise Exception("Permission denied: Unable to create table")
                return Mock()
            
            mock_connection.execute.side_effect = mock_execute
            
            warehouse.connect()
            warehouse.config['replace_existing'] = True  # Ensure we attempt to create the table
            
            table_info = {
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "FULL_REFRESH"
            }
            
            source_schema = [
                {
                    "name": "id",
                    "type": "INTEGER",
                    "nullable": False,
                    "default_value": None,
                    "primary_key": True
                }
            ]
            
            # Create table should fail due to permissions
            with pytest.raises(Exception) as exc_info:
                warehouse.create_table(table_info, source_schema, source_schema)
            
            assert "Permission denied" in str(exc_info.value)

    def test_cleanup_after_failed_operations(self, warehouse):
        """Test that cleanup operations are performed after failures"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            # Track executed queries
            executed_queries = []
            mock_schema_result = Mock()
            mock_schema_result.fetchall = Mock(return_value=[
                (0, "id", "INTEGER", "FALSE", None, "TRUE"),
                (1, "name", "VARCHAR", "TRUE", None, "FALSE")
            ])
            
            def mock_execute(sql):
                executed_queries.append(sql)
                if 'INSTALL' in sql or 'LOAD' in sql:
                    return Mock()
                elif 'PRAGMA' in sql:
                    return mock_schema_result
                elif "INSERT INTO" in sql:
                    raise Exception("Insert operation failed")
                return Mock()
            
            mock_connection.execute.side_effect = mock_execute
            
            warehouse.connect()
            table_info = {
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "FULL_REFRESH"
            }
            
            # Create mock DataFrame
            mock_df = Mock()
            mock_df.columns = ['id', 'name']
            df_batches = [mock_df]
            
            # Attempt operation that will fail
            with pytest.raises(Exception):
                warehouse.process_insert_batches(table_info, df_batches, lambda x: mock_df)
            
            # Verify cleanup was attempted
            expected_sql = [
                "INSTALL spatial;",
                "LOAD spatial;",
                "PRAGMA table_info('test_schema.test_table');",
                "INSERT INTO test_schema.test_table (SELECT id, name FROM processed_df);"
            ]
            
            for expected, actual in zip(expected_sql, executed_queries):
                assert normalize_sql(expected) == normalize_sql(actual)

    def test_connection_cleanup_after_failures(self, warehouse):
        """Test proper connection cleanup after failures"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            def mock_execute(sql):
                if 'INSTALL' in sql or 'LOAD' in sql:
                    return Mock()
                raise Exception("Connection lost")
                
            mock_connection.execute.side_effect = mock_execute
            
            try:
                warehouse.connect()
                warehouse.execute_query("SELECT * FROM test_table")
            except Exception:
                warehouse.disconnect()
            
            # Verify connection was properly cleaned up
            mock_connection.close.assert_called_once()
            assert warehouse.connection is None

    def test_transaction_rollback_on_errors(self, warehouse):
        """Test that transactions are properly rolled back on errors"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            # Track executed queries
            executed_queries = []
            mock_schema_result = Mock()
            mock_schema_result.fetchall = Mock(return_value=[
                (0, "id", "INTEGER", "FALSE", None, "TRUE"),
                (1, "name", "VARCHAR", "TRUE", None, "FALSE")
            ])
            
            def mock_execute(sql):
                executed_queries.append(sql)
                if 'INSTALL' in sql or 'LOAD' in sql:
                    return Mock()
                elif 'PRAGMA' in sql:
                    return mock_schema_result
                elif "INSERT INTO" in sql:
                    raise Exception("Insert failed")
                return Mock()
            
            mock_connection.execute.side_effect = mock_execute
            
            warehouse.connect()
            table_info = {
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "FULL_REFRESH"
            }
            
            # Create mock DataFrame
            mock_df = Mock()
            mock_df.columns = ['id', 'name']
            df_batches = [mock_df]
            
            # Attempt operation that will fail within transaction
            try:
                warehouse.begin_transaction()
                warehouse.process_insert_batches(table_info, df_batches, lambda x: mock_df)
            except Exception:
                warehouse.rollback_transaction()
            finally:
                warehouse.disconnect()
            
            # Verify transaction management
            mock_connection.begin.assert_called_once()
            mock_connection.rollback.assert_called_once()
            mock_connection.close.assert_called_once()
            
            expected_sql = [
                "INSTALL spatial;",
                "LOAD spatial;",
                "PRAGMA table_info('test_schema.test_table');",
                "INSERT INTO test_schema.test_table (SELECT id, name FROM processed_df);"
            ]
            
            for expected, actual in zip(expected_sql, executed_queries):
                assert normalize_sql(expected) == normalize_sql(actual)


class TestTableOperations:
    def test_table_creation_with_different_schemas(self, warehouse):
        """Test table creation with different schema configurations"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_exists_result = Mock()
            mock_exists_result.fetchone.return_value = None
            
            def mock_execute_return(sql):
                if "information_schema.tables" in sql:
                    return mock_exists_result
                return Mock()

            mock_connection.execute.side_effect = mock_execute_return
            mock_connect.return_value = mock_connection
            warehouse.config["replace_existing"] = True
            warehouse.connect()

            test_cases = [
                # Basic table with single PK
                {
                    "table_info": {
                        "database": "test_db",
                        "schema": "test_schema",
                        "table": "single_pk_table",
                        "cdc_type": "FULL_REFRESH"
                    },
                    "schema": [
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
                },
                # Table with composite PK
                {
                    "table_info": {
                        "database": "test_db",
                        "schema": "test_schema",
                        "table": "composite_pk_table",
                        "cdc_type": "FULL_REFRESH"
                    },
                    "schema": [
                        {
                            "name": "id1",
                            "type": "INTEGER",
                            "nullable": False,
                            "default_value": None,
                            "primary_key": True
                        },
                        {
                            "name": "id2",
                            "type": "INTEGER",
                            "nullable": False,
                            "default_value": None,
                            "primary_key": True
                        }
                    ]
                },
                # Table with all nullable columns
                {
                    "table_info": {
                        "database": "test_db",
                        "schema": "test_schema",
                        "table": "nullable_table",
                        "cdc_type": "FULL_REFRESH"
                    },
                    "schema": [
                        {
                            "name": "col1",
                            "type": "VARCHAR",
                            "nullable": True,
                            "default_value": None,
                            "primary_key": False
                        },
                        {
                            "name": "col2",
                            "type": "INTEGER",
                            "nullable": True,
                            "default_value": None,
                            "primary_key": False
                        }
                    ]
                }
            ]

            for case in test_cases:
                with patch('datetime.datetime') as mock_datetime:
                    mock_datetime.now.return_value = datetime(2024, 1, 1, 12, 0, 0)
                    mock_connection.execute.reset_mock()  # Reset the mock between cases
                    
                    warehouse.create_table(case["table_info"], case["schema"], case["schema"])

                    schema_def = ", ".join([
                        f"{col['name']} {col['type']}{' NOT NULL' if not col['nullable'] else ''}"
                        for col in case["schema"]
                    ])
                    primary_keys = [col["name"] for col in case["schema"] if col["primary_key"]]
                    pk_list = f"[{', '.join([repr(pk) for pk in primary_keys])}]"

                    table_name = f"{case['table_info']['schema']}.{case['table_info']['table']}"
                    
                    expected_calls = [
                        f"CREATE SCHEMA IF NOT EXISTS {case['table_info']['schema']};",
                        f"CREATE OR REPLACE TABLE {table_name} ({schema_def});",
                        f"DELETE FROM cdc_schema.captured_tables WHERE schema_name = '{case['table_info']['schema']}' and table_name = '{case['table_info']['table']}';",
                        f"DELETE FROM cdc_schema.source_columns WHERE table_schema = '{case['table_info']['schema']}' and table_name = '{case['table_info']['table']}';",
                        f"INSERT INTO cdc_schema.captured_tables VALUES ('{case['table_info']['schema']}', '{case['table_info']['table']}', '2024-01-01 12:00:00', '2024-01-01 12:00:00', {pk_list}, 'FULL_REFRESH');",
                        f"INSERT INTO cdc_schema.source_columns VALUES " + 
                        ", ".join([f"('test_db', '{case['table_info']['schema']}', '{case['table_info']['table']}', '{col['name']}', '{col['type']}', NULL, {'TRUE' if col['nullable'] else 'FALSE'}, {'TRUE' if col['primary_key'] else 'FALSE'})" 
                                for col in case["schema"]]) + ";"
                    ]

                    # Skip first two calls which are from connect() if this is the first case
                    # actual_calls = mock_connection.execute.call_args_list[2:] if case == test_cases[0] else mock_connection.execute.call_args_list
                    actual_calls = [call for call in mock_connection.execute.call_args_list 
                                    if not any(x in call[0][0] for x in ["INSTALL spatial", "LOAD spatial"])]
                    print(f"\nTesting case: {case['table_info']['table']}")
                    for expected, actual in zip(expected_calls, actual_calls):
                        assert normalize_sql(expected) == normalize_sql(actual[0][0])

    def test_table_truncation(self, warehouse):
        """Test table truncation operation"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "schema": "test_schema",
                "table": "test_table"
            }
            
            warehouse.truncate_table(table_info)
            
            expected_sql = "TRUNCATE TABLE test_schema.test_table;"
            # Skip first two calls which are from connect()
            actual_sql = mock_connection.execute.call_args_list[2][0][0]
            assert normalize_sql(expected_sql) == normalize_sql(actual_sql)

    def test_table_existence_checks(self, warehouse):
        """Test table existence verification"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_exists_result = Mock()
            
            # Test existing table
            mock_exists_result.fetchone.return_value = ["test_table"]
            
            def mock_execute_return(sql):
                if "information_schema.tables" in sql:
                    return mock_exists_result
                return Mock()

            mock_connection.execute.side_effect = mock_execute_return
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "schema": "test_schema",
                "table": "test_table"
            }
            
            # Test existing table
            assert warehouse.table_exists(table_info) == True
            
            # Test non-existing table
            mock_exists_result.fetchone.return_value = None
            assert warehouse.table_exists(table_info) == False
            
            expected_sql = "SELECT * FROM information_schema.tables WHERE table_schema = 'test_schema' AND table_name = 'test_table';"
            # Skip first two calls which are from connect()
            actual_sql = mock_connection.execute.call_args_list[2][0][0]
            assert normalize_sql(expected_sql) == normalize_sql(actual_sql)

    def test_special_characters_in_table_names(self, warehouse):
        """Test handling of special characters in table and schema names"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_exists_result = Mock()
            mock_exists_result.fetchone.return_value = None
            
            def mock_execute_return(sql):
                if "information_schema.tables" in sql:
                    return mock_exists_result
                return Mock()

            mock_connection.execute.side_effect = mock_execute_return
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            special_char_cases = [
                {
                    "schema": "test-schema",
                    "table": "test-table"
                },
                {
                    "schema": "test_schema",
                    "table": "test$table"
                },
                {
                    "schema": "test.schema",
                    "table": "test.table"
                }
            ]

            for table_info in special_char_cases:
                schema = [{
                    "name": "id",
                    "type": "INTEGER",
                    "nullable": False,
                    "default_value": None,
                    "primary_key": True
                }]
                
                table_info["database"] = "test_db"
                table_info["cdc_type"] = "FULL_REFRESH"

                # Reset the mock connection's call list for each iteration
                mock_connection.execute.reset_mock()

                with patch('datetime.datetime') as mock_datetime:
                    mock_datetime.now.return_value = datetime(2024, 1, 1, 12, 0, 0)
                    warehouse.create_table(table_info, schema, schema)
                    
                    # Generate expected queries for this specific table_info
                    expected_calls = [
                        # "INSTALL spatial;",
                        # "LOAD spatial;",
                        f"SELECT * FROM information_schema.tables WHERE table_schema = '{table_info['schema']}' AND table_name = '{table_info['table']}';",
                        f"CREATE SCHEMA IF NOT EXISTS {table_info['schema']};",
                        f"CREATE TABLE IF NOT EXISTS {table_info['schema']}.{table_info['table']} (id INTEGER NOT NULL);",
                        f"DELETE FROM cdc_schema.captured_tables WHERE schema_name = '{table_info['schema']}' and table_name = '{table_info['table']}';",
                        f"DELETE FROM cdc_schema.source_columns WHERE table_schema = '{table_info['schema']}' and table_name = '{table_info['table']}';",
                        f"INSERT INTO cdc_schema.captured_tables VALUES ('{table_info['schema']}', '{table_info['table']}', '2024-01-01 12:00:00', '2024-01-01 12:00:00', ['id'], 'FULL_REFRESH');",
                        f"INSERT INTO cdc_schema.source_columns VALUES ('test_db', '{table_info['schema']}', '{table_info['table']}', 'id', 'INTEGER', NULL, FALSE, TRUE);"
                    ]

                    actual_calls = mock_connection.execute.call_args_list
                    
                    print(f"\nTesting case: schema={table_info['schema']}, table={table_info['table']}")
                    for expected, actual in zip(expected_calls, actual_calls):
                        assert normalize_sql(expected) == normalize_sql(actual[0][0])

    def test_schema_and_table_name_formatting(self, warehouse):
        """Test proper formatting of schema and table names"""
        with patch('duckdb.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            
            test_cases = [
                {
                    "input": {
                        "schema": "TestSchema",
                        "table": "TestTable"
                    },
                    "expected": "TestSchema.TestTable"
                },
                {
                    "input": {
                        "schema": "test_schema",
                        "table": "test_table"
                    },
                    "expected": "test_schema.test_table"
                },
                {
                    "input": {
                        "schema": "test.schema",
                        "table": "test.table"
                    },
                    "expected": "test.schema.test.table"
                }
            ]
            
            for case in test_cases:
                result = warehouse.get_full_table_name(case["input"])
                assert result == case["expected"]