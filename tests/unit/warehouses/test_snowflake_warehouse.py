import pytest
from unittest.mock import Mock, patch, call
from src.warehouses.snowflake_warehouse import SnowflakeWarehouse
from tests.utils.helpers import normalize_sql
from datetime import datetime

@pytest.fixture
def config():
    return {
        "account": "test_account",
        "user": "test_user",
        "password": "test_password",
        "role": "test_role",
        "warehouse": "test_warehouse",
        "database": "test_database",
        "change_tracking_database": "test_cdc_db",
        "change_tracking_schema": "test_cdc_schema",
        "warehouse_role": "SOURCE",
        "replace_existing": False
    }

@pytest.fixture
def sso_config(config):
    sso_config = config.copy()
    sso_config["authenticator"] = "externalbrowser"
    del sso_config["password"]
    return sso_config

@pytest.fixture
def warehouse(config):
    return SnowflakeWarehouse(config)

class TestConnectionManagement:
    def test_connect_standard_auth(self, warehouse, config):
        """Test connection with standard username/password authentication"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            
            # Verify connection was attempted with correct credentials
            mock_connect.assert_called_once_with(
                account=config['account'],
                user=config['user'],
                password=config['password']
            )
            
            # Verify role and warehouse were set
            mock_cursor.execute.assert_has_calls([
                call(f"USE ROLE {config['role']};"),
                call(f"USE WAREHOUSE {config['warehouse']};")
            ])
            
            assert warehouse.connection == mock_connection
            assert warehouse.cursor == mock_cursor

    def test_connect_sso_auth(self, config):
        """Test connection with SSO authentication"""
        sso_config = config.copy()
        sso_config["authenticator"] = "externalbrowser"
        del sso_config["password"]
        
        warehouse = SnowflakeWarehouse(sso_config)
        
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            
            mock_connect.assert_called_once_with(
                account=sso_config['account'],
                user=sso_config['user'],
                authenticator="externalbrowser"
            )

    def test_connect_with_different_role(self, warehouse):
        """Test connecting with a non-default role"""
        different_role = "DIFFERENT_ROLE"
        
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect(different_role)
            
            mock_cursor.execute.assert_has_calls([
                call(f"USE ROLE {different_role};"),
                call(f"USE WAREHOUSE {warehouse.config['warehouse']};")
            ])

    def test_connect_with_invalid_credentials(self, warehouse):
        """Test error handling for invalid credentials"""
        with patch('snowflake.connector.connect', side_effect=Exception("Invalid credentials")):
            with pytest.raises(Exception, match="Invalid credentials"):
                warehouse.connect()
            assert warehouse.connection is None
            assert warehouse.cursor is None

    def test_disconnect(self, warehouse):
        """Test proper disconnection"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            warehouse.disconnect()
            
            mock_cursor.close.assert_called_once()
            mock_connection.close.assert_called_once()
            assert warehouse.cursor is None
            assert warehouse.connection is None

    def test_disconnect_when_not_connected(self, warehouse):
        """Test disconnection when not connected"""
        warehouse.connection = None
        warehouse.cursor = None
        warehouse.disconnect()  # Should not raise any errors

    def test_reconnect_after_disconnect(self, warehouse):
        """Test connecting after a previous disconnect"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            # First connection
            warehouse.connect()
            warehouse.disconnect()
            
            # Second connection
            warehouse.connect()
            
            assert mock_connect.call_count == 2
            assert warehouse.connection is not None
            assert warehouse.cursor is not None

class TestTransactionManagement:
    def test_begin_transaction(self, warehouse):
        """Test beginning a transaction"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            warehouse.begin_transaction()
            
            mock_cursor.execute.assert_has_calls([
                call(f"USE ROLE {warehouse.config['role']};"),
                call(f"USE WAREHOUSE {warehouse.config['warehouse']};"),
                call("BEGIN;")
            ])

    def test_commit_transaction(self, warehouse):
        """Test committing a transaction"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            warehouse.begin_transaction()
            warehouse.commit_transaction()
            
            mock_connection.commit.assert_called_once()

    def test_rollback_transaction(self, warehouse):
        """Test rolling back a transaction"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            warehouse.begin_transaction()
            warehouse.rollback_transaction()
            
            mock_connection.rollback.assert_called_once()

    def test_multiple_transactions(self, warehouse):
        """Test multiple begin/commit cycles"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            
            # First transaction
            warehouse.begin_transaction()
            warehouse.commit_transaction()
            
            # Second transaction
            warehouse.begin_transaction()
            warehouse.commit_transaction()
            
            assert mock_cursor.execute.call_count == 4  # 2 initial calls + 2 BEGIN calls
            assert mock_connection.commit.call_count == 2

    def test_transaction_after_connection_reset(self, warehouse):
        """Test transaction behavior after reconnecting"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
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
            assert mock_cursor.execute.call_count == 6  # 2 sets of (USE ROLE, USE WAREHOUSE, BEGIN)
            assert mock_connection.commit.call_count == 2

class TestSchemaAndMetadata:
    def test_get_schema(self, warehouse):
        """Test schema retrieval for a table"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            # Mock cursor.fetchall to return sample schema data
            mock_cursor.fetchall.return_value = [
                ("id", "NUMBER(38,0)", "", "N", None, "Y"),
                ("name", "VARCHAR", "", "Y", None, "N"),
                ("created_at", "TIMESTAMP_NTZ", "", "N", "CURRENT_TIMESTAMP()", "N"),
                ("metadata", "VARIANT", "", "Y", None, "N")
            ]
            
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table"
            }
            
            schema = warehouse.get_schema(table_info)
            
            # Verify DESC TABLE was called
            mock_cursor.execute.assert_has_calls([
                call(f"USE ROLE {warehouse.config['role']};"),
                call(f"USE WAREHOUSE {warehouse.config['warehouse']};"),
                call("""DESC TABLE test_db.test_schema.test_table;""")
            ])
            
            # Verify schema format
            assert len(schema) == 4
            assert schema[0] == {
                "name": "id",
                "type": "NUMBER(38,0)",
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
                "type": "TIMESTAMP_NTZ",
                "nullable": False,
                "default_value": "CURRENT_TIMESTAMP()",
                "primary_key": False
            }

    def test_get_schema_not_connected(self, warehouse):
        """Test schema retrieval when not connected"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table"
        }
        
        with pytest.raises(ConnectionError, match="You have not established a connection to the database"):
            warehouse.get_schema(table_info)

    def test_get_schema_no_cursor(self, warehouse):
        """Test schema retrieval with no cursor"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection
            warehouse.connect()
            warehouse.cursor = None
            
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table"
            }
            
            with pytest.raises(ConnectionError, match="You do not have a valid cursor"):
                warehouse.get_schema(table_info)

    def test_get_primary_keys(self, warehouse):
        """Test retrieving primary keys from schema"""
        with patch.object(warehouse, 'get_schema') as mock_get_schema:
            mock_get_schema.return_value = [
                {"name": "id", "type": "NUMBER", "nullable": False, "default_value": None, "primary_key": True},
                {"name": "email", "type": "VARCHAR", "nullable": False, "default_value": None, "primary_key": True},
                {"name": "name", "type": "VARCHAR", "nullable": True, "default_value": None, "primary_key": False}
            ]
            
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table"
            }
            
            primary_keys = warehouse.get_primary_keys(table_info)
            assert primary_keys == ["email", "id"]  # Should be sorted alphabetically

    def test_get_full_table_name(self, warehouse):
        """Test full table name generation"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table"
        }
        assert warehouse.get_full_table_name(table_info) == "test_db.test_schema.test_table"

    def test_get_change_tracking_schema_full_name(self, warehouse):
        """Test change tracking schema name generation"""
        expected = f"{warehouse.config['change_tracking_database']}.{warehouse.config['change_tracking_schema']}"
        assert warehouse.get_change_tracking_schema_full_name() == expected

    def test_format_schema_row(self, warehouse):
        """Test schema row formatting"""
        test_cases = [
            # (input_row, expected_output)
            (
                ("col1", "VARCHAR", "", "Y", None, "N"),
                {
                    "name": "col1",
                    "type": "VARCHAR",
                    "nullable": True,
                    "default_value": None,
                    "primary_key": False
                }
            ),
            (
                ("col2", "NUMBER", "", "N", "0", "Y"),
                {
                    "name": "col2",
                    "type": "NUMBER",
                    "nullable": False,
                    "default_value": "0",
                    "primary_key": True
                }
            )
        ]
        
        for input_row, expected_output in test_cases:
            assert warehouse.format_schema_row(input_row) == expected_output

    def test_get_schema_all_column_types(self, warehouse):
        """Test schema retrieval with all Snowflake column types"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            # Mock cursor.fetchall to return all Snowflake data types
            mock_cursor.fetchall.return_value = [
                # Numeric types
                ("number_col", "NUMBER(38,0)", "", "Y", None, "N"),
                ("decimal_col", "DECIMAL(10,2)", "", "Y", None, "N"),
                ("numeric_col", "NUMERIC(15,5)", "", "Y", None, "N"),
                ("int_col", "INT", "", "Y", None, "N"),
                ("integer_col", "INTEGER", "", "Y", None, "N"),
                ("bigint_col", "BIGINT", "", "Y", None, "N"),
                ("smallint_col", "SMALLINT", "", "Y", None, "N"),
                ("tinyint_col", "TINYINT", "", "Y", None, "N"),
                ("byteint_col", "BYTEINT", "", "Y", None, "N"),
                ("float_col", "FLOAT", "", "Y", None, "N"),
                ("float4_col", "FLOAT4", "", "Y", None, "N"),
                ("float8_col", "FLOAT8", "", "Y", None, "N"),
                ("double_col", "DOUBLE", "", "Y", None, "N"),
                ("double_precision_col", "DOUBLE PRECISION", "", "Y", None, "N"),
                ("real_col", "REAL", "", "Y", None, "N"),
                
                # String/Text types
                ("varchar_col", "VARCHAR(255)", "", "Y", None, "N"),
                ("char_col", "CHAR(10)", "", "Y", None, "N"),
                ("character_col", "CHARACTER(15)", "", "Y", None, "N"),
                ("string_col", "STRING", "", "Y", None, "N"),
                ("text_col", "TEXT", "", "Y", None, "N"),
                
                # Binary types
                ("binary_col", "BINARY", "", "Y", None, "N"),
                ("varbinary_col", "VARBINARY", "", "Y", None, "N"),
                
                # Boolean type
                ("boolean_col", "BOOLEAN", "", "Y", None, "N"),
                
                # Date/Time types
                ("date_col", "DATE", "", "Y", None, "N"),
                ("datetime_col", "DATETIME", "", "Y", None, "N"),
                ("time_col", "TIME", "", "Y", None, "N"),
                ("timestamp_col", "TIMESTAMP", "", "Y", None, "N"),
                ("timestamp_ltz_col", "TIMESTAMP_LTZ", "", "Y", None, "N"),
                ("timestamp_ntz_col", "TIMESTAMP_NTZ", "", "Y", None, "N"),
                ("timestamp_tz_col", "TIMESTAMP_TZ", "", "Y", None, "N"),
                
                # Semi-structured data types
                ("variant_col", "VARIANT", "", "Y", None, "N"),
                ("object_col", "OBJECT", "", "Y", None, "N"),
                ("array_col", "ARRAY", "", "Y", None, "N"),
                
                # Geospatial types
                ("geography_col", "GEOGRAPHY", "", "Y", None, "N"),
                ("geometry_col", "GEOMETRY", "", "Y", None, "N"),
                
                # Vector type
                ("vector_col", "VECTOR(FLOAT, 256)", "", "Y", None, "N")
            ]
            
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table"
            }
            
            schema = warehouse.get_schema(table_info)
            
            # Verify all types are correctly parsed
            assert len(schema) == 36  # Total number of test columns
            
            # Check specific type mappings
            type_checks = {
                "number_col": "NUMBER(38,0)",
                "decimal_col": "DECIMAL(10,2)",
                "numeric_col": "NUMERIC(15,5)",
                "int_col": "INT",
                "integer_col": "INTEGER",
                "bigint_col": "BIGINT",
                "smallint_col": "SMALLINT",
                "tinyint_col": "TINYINT",
                "byteint_col": "BYTEINT",
                "float_col": "FLOAT",
                "float4_col": "FLOAT4",
                "float8_col": "FLOAT8",
                "double_col": "DOUBLE",
                "double_precision_col": "DOUBLE PRECISION",
                "real_col": "REAL",
                "varchar_col": "VARCHAR(255)",
                "char_col": "CHAR(10)",
                "character_col": "CHARACTER(15)",
                "string_col": "STRING",
                "text_col": "TEXT",
                "binary_col": "BINARY",
                "varbinary_col": "VARBINARY",
                "boolean_col": "BOOLEAN",
                "date_col": "DATE",
                "datetime_col": "DATETIME",
                "time_col": "TIME",
                "timestamp_col": "TIMESTAMP",
                "timestamp_ltz_col": "TIMESTAMP_LTZ",
                "timestamp_ntz_col": "TIMESTAMP_NTZ",
                "timestamp_tz_col": "TIMESTAMP_TZ",
                "variant_col": "VARIANT",
                "object_col": "OBJECT",
                "array_col": "ARRAY",
                "geography_col": "GEOGRAPHY",
                "geometry_col": "GEOMETRY",
                "vector_col": "VECTOR(FLOAT, 256)"
            }
            
            for col in schema:
                if col["name"] in type_checks:
                    assert col["type"] == type_checks[col["name"]], f"Type mismatch for {col['name']}"

    def test_format_schema_row_type_variations(self, warehouse):
        """Test schema row formatting with various type specifications"""
        test_cases = [
            # Numeric types with precision/scale
            (("num1", "NUMBER(38,0)", "", "Y", None, "N"), "NUMBER(38,0)"),
            (("num2", "DECIMAL(10,2)", "", "Y", None, "N"), "DECIMAL(10,2)"),
            (("num3", "NUMERIC(15,5)", "", "Y", None, "N"), "NUMERIC(15,5)"),
            
            # String types with length
            (("str1", "VARCHAR(255)", "", "Y", None, "N"), "VARCHAR(255)"),
            (("str2", "CHAR(10)", "", "Y", None, "N"), "CHAR(10)"),
            (("str3", "CHARACTER(15)", "", "Y", None, "N"), "CHARACTER(15)"),
            
            # Timestamp types with precision
            (("ts1", "TIMESTAMP_NTZ(9)", "", "Y", None, "N"), "TIMESTAMP_NTZ(9)"),
            (("ts2", "TIMESTAMP_TZ(6)", "", "Y", None, "N"), "TIMESTAMP_TZ(6)"),
            
            # Vector type
            (("vec1", "VECTOR(FLOAT, 256)", "", "Y", None, "N"), "VECTOR(FLOAT, 256)"),
            (("vec2", "VECTOR(FLOAT, 16)", "", "Y", None, "N"), "VECTOR(FLOAT, 16)")
        ]
        
        for input_row, expected_type in test_cases:
            formatted = warehouse.format_schema_row(input_row)
            assert formatted["type"] == expected_type, f"Type mismatch for {input_row[0]}"

class TestProblemDetection:
    def test_find_problems_invalid_cdc_type(self, warehouse):
        """Test problem detection for invalid CDC type"""
        with patch.object(warehouse, 'get_schema') as mock_get_schema:
            mock_get_schema.return_value = [
                {"name": "id", "type": "NUMBER", "nullable": False, "default_value": None, "primary_key": True}
            ]
            
            tables = [{
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "INVALID_TYPE"
            }]
            
            problems = warehouse.find_problems(tables)
            assert len(problems) == 1
            assert "has an invalid cdc_type" in problems[0]

    def test_find_problems_geometry_in_standard_stream(self, warehouse):
        """Test problem detection for geometry columns in standard streams"""
        with patch.object(warehouse, 'get_schema') as mock_get_schema:
            mock_get_schema.return_value = [
                {"name": "id", "type": "NUMBER", "nullable": False, "default_value": None, "primary_key": True},
                {"name": "location", "type": "GEOMETRY", "nullable": True, "default_value": None, "primary_key": False}
            ]
            
            tables = [{
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "STANDARD_STREAM"
            }]
            
            problems = warehouse.find_problems(tables)
            assert len(problems) == 1
            assert "has a geometry or geography column" in problems[0]
            assert "Use append_only_streams or full_refresh" in problems[0]

    def test_find_problems_multiple_issues(self, warehouse):
        """Test problem detection for multiple issues"""
        with patch.object(warehouse, 'get_schema') as mock_get_schema:
            mock_get_schema.return_value = [
                {"name": "id", "type": "NUMBER", "nullable": False, "default_value": None, "primary_key": True},
                {"name": "location", "type": "GEOMETRY", "nullable": True, "default_value": None, "primary_key": False}
            ]
            
            tables = [
                {
                    "database": "test_db",
                    "schema": "test_schema",
                    "table": "table1",
                    "cdc_type": "INVALID_TYPE"
                },
                {
                    "database": "test_db",
                    "schema": "test_schema",
                    "table": "table2",
                    "cdc_type": "STANDARD_STREAM"
                }
            ]
            
            problems = warehouse.find_problems(tables)
            assert len(problems) == 2
            assert any("has an invalid cdc_type" in problem for problem in problems)
            assert any("has a geometry or geography column" in problem for problem in problems)

    def test_find_problems_no_issues(self, warehouse):
        """Test problem detection with no issues"""
        with patch.object(warehouse, 'get_schema') as mock_get_schema:
            mock_get_schema.return_value = [
                {"name": "id", "type": "NUMBER", "nullable": False, "default_value": None, "primary_key": True},
                {"name": "name", "type": "VARCHAR", "nullable": True, "default_value": None, "primary_key": False}
            ]
            
            tables = [{
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "STANDARD_STREAM"
            }]
            
            problems = warehouse.find_problems(tables)
            assert len(problems) == 0

    def test_has_geometry_or_geography_column(self, warehouse):
        """Test detection of geometry and geography columns"""
        test_cases = [
            # (schema, expected_result)
            (
                [{"name": "id", "type": "NUMBER"}],
                False
            ),
            (
                [{"name": "location", "type": "GEOMETRY"}],
                True
            ),
            (
                [{"name": "region", "type": "GEOGRAPHY"}],
                True
            ),
            (
                [
                    {"name": "id", "type": "NUMBER"},
                    {"name": "location", "type": "GEOMETRY"},
                    {"name": "name", "type": "VARCHAR"}
                ],
                True
            ),
            (
                [
                    {"name": "id", "type": "NUMBER"},
                    {"name": "name", "type": "VARCHAR"}
                ],
                False
            )
        ]
        
        for schema, expected_result in test_cases:
            assert warehouse.has_geometry_or_geography_column(schema) == expected_result

class TestCDCSetup:
    def test_setup_environment_valid_tables(self, warehouse):
        """Test successful setup of CDC environment with valid tables"""
        with patch.object(warehouse, 'get_schema') as mock_get_schema:
            mock_get_schema.return_value = [
                {"name": "id", "type": "NUMBER", "nullable": False, "default_value": None, "primary_key": True},
                {"name": "data", "type": "VARCHAR", "nullable": True, "default_value": None, "primary_key": False}
            ]
            
            tables = [
                {
                    "database": "test_db",
                    "schema": "test_schema",
                    "table": "standard_stream_table",
                    "cdc_type": "STANDARD_STREAM"
                },
                {
                    "database": "test_db",
                    "schema": "test_schema",
                    "table": "append_stream_table",
                    "cdc_type": "APPEND_ONLY_STREAM"
                },
                {
                    "database": "test_db",
                    "schema": "test_schema",
                    "table": "full_refresh_table",
                    "cdc_type": "FULL_REFRESH"
                }
            ]
            
            with patch.object(warehouse, '_create_stream_objects') as mock_create_stream:
                warehouse.setup_environment(tables)
                
                # Verify _create_stream_objects was called only for stream-based tables
                assert mock_create_stream.call_count == 2
                mock_create_stream.assert_any_call(tables[0])  # STANDARD_STREAM
                mock_create_stream.assert_any_call(tables[1])  # APPEND_ONLY_STREAM

    def test_setup_environment_invalid_tables(self, warehouse):
        """Test setup fails properly with invalid table configurations"""
        with patch.object(warehouse, 'get_schema') as mock_get_schema:
            # Mock geometry column in schema
            mock_get_schema.return_value = [
                {"name": "id", "type": "NUMBER", "nullable": False, "default_value": None, "primary_key": True},
                {"name": "location", "type": "GEOMETRY", "nullable": True, "default_value": None, "primary_key": False}
            ]
            
            tables = [{
                "database": "test_db",
                "schema": "test_schema",
                "table": "invalid_table",
                "cdc_type": "STANDARD_STREAM"
            }]
            
            with pytest.raises(ValueError) as exc_info:
                warehouse.setup_environment(tables)
            
            error_msg = str(exc_info.value)
            assert "has a geometry or geography column" in error_msg
            assert "Use append_only_streams or full_refresh" in error_msg

    def test_create_stream_objects_standard_stream(self, warehouse):
        """Test creation of stream objects for standard CDC"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "STANDARD_STREAM"
            }
            
            warehouse._create_stream_objects(table_info)
            
            stream_name = warehouse.get_stream_name(table_info)
            processing_table = warehouse.get_stream_processing_table_name(table_info)
            
            # Verify all required SQL statements were executed
            expected_calls = [
                call(f"CREATE STREAM IF NOT EXISTS {stream_name} ON TABLE {warehouse.get_full_table_name(table_info)} SHOW_INITIAL_ROWS = true APPEND_ONLY = FALSE;"),
                call(f"CREATE TABLE {processing_table} IF NOT EXISTS LIKE {warehouse.get_full_table_name(table_info)};"),
                call(f'ALTER TABLE {processing_table} ADD COLUMN IF NOT EXISTS "METADATA$ACTION" varchar;'),
                call(f'ALTER TABLE {processing_table} ADD COLUMN IF NOT EXISTS "METADATA$ISUPDATE" varchar;'),
                call(f'ALTER TABLE {processing_table} ADD COLUMN IF NOT EXISTS "METADATA$ROW_ID" varchar;'),
                call(f'ALTER TABLE {processing_table} ADD COLUMN IF NOT EXISTS etl_id varchar;')
            ]
            
            assert mock_cursor.execute.call_args_list[2:] == expected_calls

    def test_create_stream_objects_append_only(self, warehouse):
        """Test creation of stream objects for append-only CDC"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "APPEND_ONLY_STREAM"
            }
            
            warehouse._create_stream_objects(table_info)
            
            stream_name = warehouse.get_stream_name(table_info)
            processing_table = warehouse.get_stream_processing_table_name(table_info)
            
            # Verify all required SQL statements were executed
            expected_calls = [
                call(f"CREATE STREAM IF NOT EXISTS {stream_name} ON TABLE {warehouse.get_full_table_name(table_info)} SHOW_INITIAL_ROWS = true APPEND_ONLY = TRUE;"),
                call(f"CREATE TABLE {processing_table} IF NOT EXISTS LIKE {warehouse.get_full_table_name(table_info)};"),
                call(f'ALTER TABLE {processing_table} ADD COLUMN IF NOT EXISTS "METADATA$ACTION" varchar;'),
                call(f'ALTER TABLE {processing_table} ADD COLUMN IF NOT EXISTS "METADATA$ISUPDATE" varchar;'),
                call(f'ALTER TABLE {processing_table} ADD COLUMN IF NOT EXISTS "METADATA$ROW_ID" varchar;'),
                call(f'ALTER TABLE {processing_table} ADD COLUMN IF NOT EXISTS etl_id varchar;')
            ]
            
            assert mock_cursor.execute.call_args_list[2:] == expected_calls

    def test_create_stream_objects_replace_existing(self, warehouse):
        """Test creation of stream objects with replace_existing=False"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.config['replace_existing'] = True
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "STANDARD_STREAM"
            }
            
            warehouse._create_stream_objects(table_info)
            
            stream_name = warehouse.get_stream_name(table_info)
            processing_table = warehouse.get_stream_processing_table_name(table_info)
            
            # Verify all required SQL statements were executed with IF NOT EXISTS
            expected_calls = [
                call(f"CREATE OR REPLACE STREAM {stream_name} ON TABLE {warehouse.get_full_table_name(table_info)} SHOW_INITIAL_ROWS = true APPEND_ONLY = FALSE;"),
                call(f"CREATE OR REPLACE TABLE {processing_table} LIKE {warehouse.get_full_table_name(table_info)};"),
                call(f'ALTER TABLE {processing_table} ADD COLUMN IF NOT EXISTS "METADATA$ACTION" varchar;'),
                call(f'ALTER TABLE {processing_table} ADD COLUMN IF NOT EXISTS "METADATA$ISUPDATE" varchar;'),
                call(f'ALTER TABLE {processing_table} ADD COLUMN IF NOT EXISTS "METADATA$ROW_ID" varchar;'),
                call(f'ALTER TABLE {processing_table} ADD COLUMN IF NOT EXISTS etl_id varchar;')
            ]
            
            assert mock_cursor.execute.call_args_list[2:] == expected_calls

    def test_stream_name_generation(self, warehouse):
        """Test stream name generation follows expected pattern"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table"
        }
        
        expected_stream = f"{warehouse.get_change_tracking_schema_full_name()}.test_db$test_schema$test_table"
        assert warehouse.get_stream_name(table_info) == expected_stream
        
        # Test with special characters
        table_info_special = {
            "database": "test.db",
            "schema": "test-schema",
            "table": "test_table$special"
        }
        
        expected_stream_special = f"{warehouse.get_change_tracking_schema_full_name()}.test.db$test-schema$test_table$special"
        assert warehouse.get_stream_name(table_info_special) == expected_stream_special

    def test_processing_table_name_generation(self, warehouse):
        """Test processing table name generation follows expected pattern"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table"
        }
        
        expected_table = f"{warehouse.get_change_tracking_schema_full_name()}.test_db$test_schema$test_table_processing"
        assert warehouse.get_stream_processing_table_name(table_info) == expected_table
        
        # Test with special characters
        table_info_special = {
            "database": "test.db",
            "schema": "test-schema",
            "table": "test_table$special"
        }
        
        expected_table_special = f"{warehouse.get_change_tracking_schema_full_name()}.test.db$test-schema$test_table$special_processing"
        assert warehouse.get_stream_processing_table_name(table_info_special) == expected_table_special


class TestCDCOperations:
    def test_prepare_stream_ingestion(self, warehouse):
        """Test preparation of stream ingestion with new and existing ETL IDs"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table"
            }
            
            etl_id = "test-etl-id"
            completed_etl_ids = ["old-etl-1", "old-etl-2"]
            
            warehouse.prepare_stream_ingestion(table_info, etl_id, completed_etl_ids)
            
            stream_processing_table = warehouse.get_stream_processing_table_name(table_info)
            stream_name = warehouse.get_stream_name(table_info)
            
            expected_calls = [
                # Specify the role and warehouse to be used
                call(f"USE ROLE test_role;"),
                call(f"USE WAREHOUSE test_warehouse;"),
                # Delete already processed records
                call(f"DELETE FROM {stream_processing_table} WHERE etl_id in ('old-etl-1', 'old-etl-2');"),
                # Insert new records from stream
                call(f"INSERT INTO {stream_processing_table} SELECT *, '{etl_id}' FROM {stream_name};"),
                # Update ETL ID for all records
                call(f"UPDATE {stream_processing_table} SET etl_id = '{etl_id}';")
            ]
            
            # assert mock_cursor.execute.call_args_list == expected_calls
            for actual_call, expected_call in zip(mock_cursor.execute.call_args_list, expected_calls):
                assert normalize_sql(actual_call.args[0]) == normalize_sql(expected_call.args[0])

    def test_prepare_stream_ingestion_no_completed_etls(self, warehouse):
        """Test stream ingestion preparation with no previously completed ETL IDs"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table"
            }
            
            etl_id = "test-etl-id"
            completed_etl_ids = []
            
            warehouse.prepare_stream_ingestion(table_info, etl_id, completed_etl_ids)
            
            stream_processing_table = warehouse.get_stream_processing_table_name(table_info)
            stream_name = warehouse.get_stream_name(table_info)
            
            expected_calls = [
                # Specify the role and warehouse to be used
                call(f"USE ROLE test_role;"),
                call(f"USE WAREHOUSE test_warehouse;"),
                # Insert new records from stream
                call(f"INSERT INTO {stream_processing_table} SELECT *, '{etl_id}' FROM {stream_name};"),
                # Update ETL ID for all records
                call(f"UPDATE {stream_processing_table} SET etl_id = '{etl_id}';")
            ]
            
            for actual_call, expected_call in zip(mock_cursor.execute.call_args_list, expected_calls):
                assert normalize_sql(actual_call.args[0]) == normalize_sql(expected_call.args[0])

    def test_cleanup_source_standard_stream(self, warehouse):
        """Test cleanup of source after successful processing for standard stream"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "STANDARD_STREAM"
            }
            
            warehouse.cleanup_source(table_info)
            
            stream_processing_table = warehouse.get_stream_processing_table_name(table_info)
            expected_call = call(f"TRUNCATE TABLE {stream_processing_table};")
            assert mock_cursor.execute.call_args == expected_call

    def test_cleanup_source_missing_table(self, warehouse):
        """Test cleanup behavior when processing table is missing"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            
            # Set up the mock to only raise the exception for the truncate command
            def side_effect(sql):
                if "TRUNCATE TABLE" in sql:
                    raise Exception("002003 SQL compilation error: Table does not exist")
                return None
                
            mock_cursor.execute.side_effect = side_effect
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "STANDARD_STREAM"
            }
            
            with pytest.raises(Exception) as exc_info:
                warehouse.cleanup_source(table_info)
            
            assert "Stream processing table not found" in str(exc_info.value)
            assert "Please run 'python main.py setup'" in str(exc_info.value)

    def test_get_delete_batches_for_stream(self, warehouse):
        """Test retrieval of delete batches from stream processing table"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            # Mock returning primary keys
            with patch.object(warehouse, 'get_primary_keys', return_value=['id', 'secondary_id']):
                warehouse.connect()
                table_info = {
                    "database": "test_db",
                    "schema": "test_schema",
                    "table": "test_table"
                }
                
                warehouse.get_delete_batches_for_stream(table_info)
                
                stream_processing_table = warehouse.get_stream_processing_table_name(table_info)
                expected_query = f"""
                    SELECT id, secondary_id
                    FROM {stream_processing_table}
                    WHERE "METADATA$ACTION" = 'DELETE';
                """
                
                assert normalize_sql(mock_cursor.execute.call_args[0][0]) == normalize_sql(expected_query)
                mock_cursor.fetch_pandas_batches.assert_called_once()

    def test_get_delete_batches_no_primary_keys(self, warehouse):
        """Test retrieval of delete batches when table has no primary keys"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            # Mock returning no primary keys
            with patch.object(warehouse, 'get_primary_keys', return_value=[]):
                warehouse.connect()
                table_info = {
                    "database": "test_db",
                    "schema": "test_schema",
                    "table": "test_table"
                }
                
                warehouse.get_delete_batches_for_stream(table_info)
                
                stream_processing_table = warehouse.get_stream_processing_table_name(table_info)
                expected_query = f"""
                    SELECT METADATA$ROW_ID as MELCHI_ROW_ID
                    FROM {stream_processing_table}
                    WHERE "METADATA$ACTION" = 'DELETE';
                """
                
                assert normalize_sql(mock_cursor.execute.call_args[0][0]) == normalize_sql(expected_query)
                mock_cursor.fetch_pandas_batches.assert_called_once()

    def test_get_insert_batches_for_stream(self, warehouse):
        """Test retrieval of insert batches from stream processing table"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            # Mock column names
            with patch.object(warehouse, '_get_column_names', return_value=['col1', 'col2']):
                warehouse.connect()
                table_info = {
                    "database": "test_db",
                    "schema": "test_schema",
                    "table": "test_table"
                }
                
                warehouse.get_insert_batches_for_stream(table_info)
                
                stream_processing_table = warehouse.get_stream_processing_table_name(table_info)
                expected_query = f"""
                    SELECT col1, col2, METADATA$ROW_ID as MELCHI_ROW_ID 
                    FROM {stream_processing_table}
                    WHERE "METADATA$ACTION" = 'INSERT';
                """
                
                assert normalize_sql(mock_cursor.execute.call_args[0][0]) == normalize_sql(expected_query)
                mock_cursor.fetch_pandas_batches.assert_called_once()

    def test_get_batches_for_full_refresh(self, warehouse):
        """Test retrieval of all records for full refresh"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table"
            }
            
            warehouse.get_batches_for_full_refresh(table_info)
            
            expected_query = f"SELECT * FROM {warehouse.get_full_table_name(table_info)};"
            assert normalize_sql(mock_cursor.execute.call_args[0][0]) == normalize_sql(expected_query)
            mock_cursor.fetch_pandas_batches.assert_called_once()

class TestSQLGeneration:
    def test_generate_source_sql_basic(self, warehouse):
        """Test basic SQL generation with minimal table configuration"""
        tables = [{
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table"
        }]
        
        sql = warehouse.generate_source_sql(tables)
        expected_statements = [
            "--This command creates the change tracking schema.  Not required if it already exists.",
            f"CREATE SCHEMA IF NOT EXISTS {warehouse.get_change_tracking_schema_full_name()};",
            "",
            "",
            "--These grants enable Melchi to create objects that track changes.",
            f"GRANT USAGE ON WAREHOUSE {warehouse.config['warehouse']} TO ROLE {warehouse.config['role']};",
            f"GRANT USAGE ON DATABASE {warehouse.config['change_tracking_database']} TO ROLE {warehouse.config['role']};",
            f"GRANT USAGE, CREATE TABLE, CREATE STREAM ON SCHEMA {warehouse.get_change_tracking_schema_full_name()} TO ROLE {warehouse.config['role']};",
            "",
            "",
            "--These grants enable Melchi to read changes from your objects.",
            "GRANT USAGE ON DATABASE test_db TO ROLE test_role;",
            "GRANT USAGE ON SCHEMA test_db.test_schema TO ROLE test_role;",
            "GRANT SELECT ON TABLE test_db.test_schema.test_table TO ROLE test_role;"
        ]
        
        generated_lines = sql.strip().split('\n')
        for generated, expected in zip(generated_lines, expected_statements):
            assert normalize_sql(generated) == normalize_sql(expected)

    def test_generate_source_sql_multiple_tables(self, warehouse):
        """Test SQL generation with multiple tables"""
        tables = [
            {
                "database": "db1",
                "schema": "schema1",
                "table": "table1"
            },
            {
                "database": "db1",
                "schema": "schema1",
                "table": "table2"
            },
            {
                "database": "db2",
                "schema": "schema2",
                "table": "table3"
            }
        ]
        
        sql = warehouse.generate_source_sql(tables)
        
        # Check for database grants (should be unique)
        assert sql.count("GRANT USAGE ON DATABASE db1") == 1
        assert sql.count("GRANT USAGE ON DATABASE db2") == 1
        
        # Check for schema grants (should be unique)
        assert sql.count("GRANT USAGE ON SCHEMA db1.schema1") == 1
        assert sql.count("GRANT USAGE ON SCHEMA db2.schema2") == 1
        
        # Check for table grants (one per table)
        assert sql.count("GRANT SELECT ON TABLE db1.schema1.table1") == 1
        assert sql.count("GRANT SELECT ON TABLE db1.schema1.table2") == 1
        assert sql.count("GRANT SELECT ON TABLE db2.schema2.table3") == 1

    def test_generate_source_sql_empty_tables(self, warehouse):
        """Test SQL generation with empty table list"""
        tables = []
        sql = warehouse.generate_source_sql(tables)
        
        # Should still generate schema and general grants
        assert "CREATE SCHEMA IF NOT EXISTS" in sql
        assert "GRANT USAGE ON WAREHOUSE" in sql
        assert "GRANT USAGE ON DATABASE" in sql
        assert "GRANT USAGE, CREATE TABLE, CREATE STREAM ON SCHEMA" in sql
        
        # Should not generate any table-specific grants
        assert "GRANT SELECT ON TABLE" not in sql

    def test_generate_source_sql_with_cdc_types(self, warehouse):
        """Test SQL generation with different CDC types"""
        tables = [
            {
                "database": "test_db",
                "schema": "test_schema",
                "table": "standard_stream_table",
                "cdc_type": "STANDARD_STREAM"
            },
            {
                "database": "test_db",
                "schema": "test_schema",
                "table": "append_only_table",
                "cdc_type": "APPEND_ONLY_STREAM"
            },
            {
                "database": "test_db",
                "schema": "test_schema",
                "table": "full_refresh_table",
                "cdc_type": "FULL_REFRESH"
            }
        ]
        
        sql = warehouse.generate_source_sql(tables)
        # Should generate same grants regardless of CDC type
        for table in tables:
            table_name = f"{table['database']}.{table['schema']}.{table['table']}"
            assert f"GRANT SELECT ON TABLE {table_name}" in sql

        # General setup should be present
        assert "CREATE SCHEMA IF NOT EXISTS" in sql
        assert f"GRANT USAGE, CREATE TABLE, CREATE STREAM ON SCHEMA {warehouse.get_change_tracking_schema_full_name()}" in sql

class TestErrorHandling:
    def test_missing_permissions(self, warehouse):
        """Test behavior when warehouse operations fail due to missing permissions"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            
            # Simulate permission errors for different operations
            def raise_permission_error(sql):
                if "CREATE STREAM" in sql:
                    raise Exception("Insufficient privileges to execute CREATE STREAM")
                elif "CREATE TABLE" in sql:
                    raise Exception("Insufficient privileges to execute CREATE TABLE")
                return None
                
            mock_cursor.execute.side_effect = raise_permission_error
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "STANDARD_STREAM"
            }
            
            # Test stream creation failure
            with pytest.raises(Exception) as exc_info:
                warehouse._create_stream_objects(table_info)
            assert "Insufficient privileges to execute CREATE STREAM" in str(exc_info.value)

    def test_invalid_table_configurations(self, warehouse):
        """Test handling of invalid table configurations"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            
            # Test with non-existent table
            mock_cursor.execute.side_effect = Exception("Object 'TEST_DB.TEST_SCHEMA.NONEXISTENT_TABLE' does not exist")
            
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "nonexistent_table",
                "cdc_type": "STANDARD_STREAM"
            }
            
            with pytest.raises(Exception) as exc_info:
                warehouse.get_schema(table_info)
            assert "Object 'TEST_DB.TEST_SCHEMA.NONEXISTENT_TABLE' does not exist" in str(exc_info.value)
            
            # Test with invalid database
            mock_cursor.execute.side_effect = Exception("Database 'INVALID_DB' does not exist")
            
            invalid_db_table = {
                "database": "invalid_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "STANDARD_STREAM"
            }
            
            with pytest.raises(Exception) as exc_info:
                warehouse.get_schema(invalid_db_table)
            assert "Database 'INVALID_DB' does not exist" in str(exc_info.value)

    def test_cleanup_after_failed_operations(self, warehouse):
        """Test cleanup behavior after failed operations"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "STANDARD_STREAM"
            }
            
            # Test stream processing cleanup after failed operation
            calls = []
            def side_effect(sql):
                calls.append(sql)
                # Allow USE ROLE and USE WAREHOUSE commands
                if sql.startswith("USE"):
                    return None
                # Fail on INSERT operation
                if sql.startswith("INSERT"):
                    raise Exception("Operation failed")
                return None
                
            mock_cursor.execute.side_effect = side_effect
            
            with pytest.raises(Exception) as exc_info:
                warehouse.prepare_stream_ingestion(table_info, "test-etl-id", [])
            assert "Operation failed" in str(exc_info.value)
            
            # Reset side effect for cleanup
            mock_cursor.execute.reset_mock()
            mock_cursor.execute.side_effect = None
            
            warehouse.cleanup_source(table_info)
            
            # Verify cleanup was attempted
            expected_cleanup_call = call(
                f"TRUNCATE TABLE {warehouse.get_stream_processing_table_name(table_info)};"
            )
            assert expected_cleanup_call in mock_cursor.execute.call_args_list

    def test_transaction_rollback_on_failure(self, warehouse):
        """Test that transactions are properly rolled back on failure"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "database": "test_db",
                "schema": "test_schema",
                "table": "test_table",
                "cdc_type": "STANDARD_STREAM"
            }
            
            # Simulate failure during stream ingestion
            mock_cursor.execute.side_effect = Exception("Failed during stream ingestion")
            
            try:
                warehouse.begin_transaction()
                warehouse.prepare_stream_ingestion(table_info, "test-etl-id", [])
            except Exception:
                warehouse.rollback_transaction()
            
            # Verify rollback was called
            mock_connection.rollback.assert_called_once()

    def test_connection_cleanup_after_failure(self, warehouse):
        """Test that connections are properly cleaned up after failures"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            # Test cleanup after failed operation
            mock_cursor.execute.side_effect = Exception("Operation failed")
            
            try:
                warehouse.connect()
                warehouse.begin_transaction()
                warehouse.prepare_stream_ingestion({
                    "database": "test_db",
                    "schema": "test_schema",
                    "table": "test_table",
                    "cdc_type": "STANDARD_STREAM"
                }, "test-etl-id", [])
            except Exception:
                warehouse.disconnect()
            
            # Verify connection cleanup
            mock_cursor.close.assert_called_once()
            mock_connection.close.assert_called_once()
            assert warehouse.connection is None
            assert warehouse.cursor is None

class TestTableOperations:
    def test_table_creation_with_different_schemas(self, warehouse):
        """Test creating a table with different schema configurations"""
        with patch('snowflake.connector.connect') as mock_connect:
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

                    actual_calls = [call for call in mock_connection.execute.call_args_list 
                                    if not any(x in call[0][0] for x in ["USE ROLE", "USE WAREHOUSE"])]
                    
                    print(f"\nTesting case: {case['table_info']['table']}")
                    for expected, actual in zip(expected_calls, actual_calls):
                        assert normalize_sql(expected) == normalize_sql(actual[0][0])

    def test_table_truncation(self, warehouse):
        """Test table truncation operation"""
        with patch('snowflake.connector.connect') as mock_connect:
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            warehouse.connect()
            table_info = {
                "database": "test_database",
                "schema": "test_schema",
                "table": "test_table"
            }
            
            warehouse.truncate_table(table_info)
            
            expected_sql = "TRUNCATE TABLE test_database.test_schema.test_table;"
            actual_sql = mock_cursor.execute.call_args_list[2][0][0]  # Skip first two setup calls
            assert normalize_sql(expected_sql) == normalize_sql(actual_sql)

    def test_special_characters_in_table_names(self, warehouse):
        """Test handling of special characters in table and schema names"""
        with patch('snowflake.connector.connect') as mock_connect:
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
                    
                    expected_calls = [
                        f"CREATE SCHEMA IF NOT EXISTS {table_info['schema']};",
                        f"CREATE OR REPLACE TABLE {table_info['schema']}.{table_info['table']} (id INTEGER NOT NULL);",
                        f"DELETE FROM cdc_schema.captured_tables WHERE schema_name = '{table_info['schema']}' and table_name = '{table_info['table']}';",
                        f"DELETE FROM cdc_schema.source_columns WHERE table_schema = '{table_info['schema']}' and table_name = '{table_info['table']}';",
                        f"INSERT INTO cdc_schema.captured_tables VALUES ('{table_info['schema']}', '{table_info['table']}', '2024-01-01 12:00:00', '2024-01-01 12:00:00', ['id'], 'FULL_REFRESH');",
                        f"INSERT INTO cdc_schema.source_columns VALUES ('test_db', '{table_info['schema']}', '{table_info['table']}', 'id', 'INTEGER', NULL, FALSE, TRUE);"
                    ]
                    
                    actual_calls = [call for call in mock_connection.execute.call_args_list 
                                    if not any(x in call[0][0] for x in ["USE ROLE", "USE WAREHOUSE"])]
                    
                    print(f"\nTesting case: schema={table_info['schema']}, table={table_info['table']}")
                    for expected, actual in zip(expected_calls, actual_calls):
                        assert normalize_sql(expected) == normalize_sql(actual[0][0])

    def test_schema_and_table_name_formatting(self, warehouse):
        """Test proper formatting of schema and table names"""
        test_cases = [
            {
                "input": {
                    "database": "TestDB",
                    "schema": "TestSchema",
                    "table": "TestTable"
                },
                "expected": "TestDB.TestSchema.TestTable"
            },
            {
                "input": {
                    "database": "test_db",
                    "schema": "test_schema",
                    "table": "test_table"
                },
                "expected": "test_db.test_schema.test_table"
            }
        ]
        
        for case in test_cases:
            result = warehouse.get_full_table_name(case["input"])
            assert result == case["expected"]