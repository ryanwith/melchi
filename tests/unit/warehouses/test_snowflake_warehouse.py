import pytest
from unittest.mock import Mock, patch, call
from src.warehouses.snowflake_warehouse import SnowflakeWarehouse
from pprint import pp

class TestSnowflakeWarehouse:
    @pytest.fixture
    def mock_snowflake_connector(self):
        with patch('snowflake.connector.connect') as mock_connect:
            yield mock_connect

    @pytest.fixture
    def snowflake_source_warehouse(self):
        config = {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
            "role": "test_role",
            "warehouse": "test_warehouse",
            "database": "test_database",
            "change_tracking_database": "test_cdc_db",
            "change_tracking_schema": "test_cdc_schema",
            "cdc_strategy": "cdc_streams",
            "warehouse_role": "SOURCE",
            "replace_existing": False
        }
        return SnowflakeWarehouse(config)
    

    # Connection tests
    def test_connect(self, snowflake_source_warehouse, mock_snowflake_connector):
        """Test that database connection is established with correct credentials"""
        """Test that USE ROLE and USE WAREHOUSE are called"""

        # Arrange
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_snowflake_connector.return_value = mock_connection

        # Act
        snowflake_source_warehouse.connect()

        # Assert
        mock_snowflake_connector.assert_called_once_with(
            account="test_account",
            user="test_user",
            password="test_password",
            # Add other config parameters as needed
        )
        assert snowflake_source_warehouse.connection == mock_connection
        assert snowflake_source_warehouse.cursor == mock_cursor
        mock_cursor.execute.assert_any_call("USE ROLE test_role;")
        mock_cursor.execute.assert_any_call("USE WAREHOUSE test_warehouse;")

    def test_disconnect(self, snowflake_source_warehouse, mock_snowflake_connector):
        """Test that the database connection is closed"""
        """Test that the snowflake_source_warehouse connection and cursor are set to None"""
        # Arrange
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_snowflake_connector.return_value = mock_connection
        snowflake_source_warehouse.connect()

        # Act
        snowflake_source_warehouse.disconnect()

        # Assert
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
        assert snowflake_source_warehouse.cursor is None
        assert snowflake_source_warehouse.connection is None

    def test_disconnect_when_not_connected(self, snowflake_source_warehouse):
        """Test that no errors are thrown"""
        # Arrange
        snowflake_source_warehouse.cursor = None
        snowflake_source_warehouse.connection = None

        # Act
        snowflake_source_warehouse.disconnect()

        # Assert
        # This test passes if no exception is raised

    # Transaction management tests
    @patch('snowflake.connector.connect')
    def test_begin_transaction(self, mock_connect, snowflake_source_warehouse):
        mock_cursor = Mock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        snowflake_source_warehouse.connect()
        snowflake_source_warehouse.begin_transaction()
        
        mock_cursor.execute.assert_has_calls([call("BEGIN;")])

    @patch('snowflake.connector.connect')
    def test_commit_transaction(self, mock_connect, snowflake_source_warehouse):
        mock_cursor = Mock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        snowflake_source_warehouse.connect()
        snowflake_source_warehouse.commit_transaction()
        
        mock_connect.return_value.commit.assert_called_once()

    @patch('snowflake.connector.connect')
    def test_rollback_transaction(self, mock_connect, snowflake_source_warehouse):
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        snowflake_source_warehouse.connect()
        snowflake_source_warehouse.rollback_transaction()
        
        mock_connection.rollback.assert_called_once()

    # Schema and table management tests

    @patch('snowflake.connector.connect')
    def test_get_schema(self, mock_connect, snowflake_source_warehouse):
        """Test that the get schema query is sent to Snowflake properly"""
        """Test that the formatted schema is returned as expected"""

        # Set up mock connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Mock the fetchall result
        mock_cursor.fetchall.return_value = [
            ("column1", "VARCHAR", "", "Y", None, "N"),
            ("column2", "INTEGER", "", "N", None, "Y")
        ]

        # Explicitly connect before calling get_schema
        snowflake_source_warehouse.connect()

        # Call get_schema
        table_info = {"database": "test_db", "schema": "test_schema", "table": "test_table"}
        schema = snowflake_source_warehouse.get_schema(table_info)

        # Assertions
        mock_cursor.execute.assert_called_with(f"DESC TABLE {snowflake_source_warehouse.get_full_table_name(table_info)}")
        
        assert len(schema) == 2
        assert schema[0] == {
            "name": "column1", "type": "VARCHAR", "nullable": True, 
            "default_value": None, "primary_key": False
        }
        assert schema[1] == {
            "name": "column2", "type": "INTEGER", "nullable": False, 
            "default_value": None, "primary_key": True
        }

    def test_get_full_table_name(self, snowflake_source_warehouse):
        """Test table name formatting"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table"
        }
        assert snowflake_source_warehouse.get_full_table_name(table_info) == "test_db.test_schema.test_table"

    def test_replace_existing(self, snowflake_source_warehouse):
        """Test that replace tables defaults to false.  Test that it works appropriately when set."""
        # Test with default config
        assert snowflake_source_warehouse.replace_existing() == False
        
        # Test with explicit config
        snowflake_source_warehouse.config['replace_existing'] = True
        assert snowflake_source_warehouse.replace_existing() == True
        snowflake_source_warehouse.config['replace_existing'] = False
        assert snowflake_source_warehouse.replace_existing() == False

    # Change Tracking Tests
    def test_get_change_tracking_schema_full_name(self, snowflake_source_warehouse):
        expected = f"{snowflake_source_warehouse.config['change_tracking_database']}.{snowflake_source_warehouse.config['change_tracking_schema']}"
        assert snowflake_source_warehouse.get_change_tracking_schema_full_name() == expected

    def test_get_stream_name(self, snowflake_source_warehouse):
        """Test stream name generation follows expected pattern"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema", 
            "table": "test_table"
        }
        expected = f"{snowflake_source_warehouse.get_change_tracking_schema_full_name()}.test_db$test_schema$test_table"
        assert snowflake_source_warehouse.get_stream_name(table_info) == expected

    def test_get_stream_processing_table_name(self, snowflake_source_warehouse):
        """Test processing table name generation follows expected pattern"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table"
        }
        expected = f"{snowflake_source_warehouse.get_change_tracking_schema_full_name()}.test_db$test_schema$test_table_processing"
        assert snowflake_source_warehouse.get_stream_processing_table_name(table_info) == expected

    def test_setup_environment_no_tables(self, snowflake_source_warehouse):
        """Test environment setup fails without tables"""
        with pytest.raises(Exception, match="No tables to transfer found"):
            snowflake_source_warehouse.setup_environment([])

    @patch('snowflake.connector.connect')
    def test_setup_environment_invalid_cdc_type(self, mock_connect, snowflake_source_warehouse):
        """Test invalid CDC strategies are caught"""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        # Set up different schema responses based on table name
        def mock_fetchall_response():
            # Get the most recent execute call's arguments
            last_query = mock_cursor.execute.call_args[0][0]
            if "standard_stream_with_geo" in last_query or "all_good" in last_query:
                return [["col1", "GEOMETRY", "", "Y", None, "N"]]
            return [["col1", "VARCHAR", "", "Y", None, "N"]]
        
        mock_cursor.fetchall.side_effect = mock_fetchall_response

        # Connect and prepare test data
        snowflake_source_warehouse.connect()
        tables = [
            {
                "table": "invalid_cdc_table",
                "schema": "schema_name",
                "database": "db_name",
                "cdc_type": "INVALID_CDC_TYPE"
            },
            {
                "table": "standard_stream_with_geo",
                "schema": "schema_name",
                "database": "db_name",
                "cdc_type": "STANDARD_STREAM"
            },
            {
                "table": "all_good",
                "schema": "schema_name",
                "database": "db_name",
                "cdc_type": "FULL_REFRESH"
            },
        ]

        expected_error_message = "\n".join((
            "The following problems were found:",
            "db_name.schema_name.invalid_cdc_table has an invalid cdc_type: INVALID_CDC_TYPE.  Valid values are append_only_stream, standard_stream, and full_refresh.",
            "db_name.schema_name.standard_stream_with_geo has a geometry or geography column.  Snowflake does not support these in standard streams.  Use append_only_streams or full_refresh for tables with these columns.",
        ))

        with pytest.raises(ValueError, match=expected_error_message):
            snowflake_source_warehouse.setup_source_environment(tables)

    @patch('snowflake.connector.connect')
    def test_create_cdc_objects_standard_cdc(self, mock_connect, snowflake_source_warehouse):
        """Test creation of CDC objects for standard CDC type"""
        # Set up mocks
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        # Connect and prepare test data
        snowflake_source_warehouse.connect()
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "cdc_type": "STANDARD_STREAM"
        }

        snowflake_source_warehouse.config['replace_existing'] = True

        # Execute the method
        snowflake_source_warehouse.create_stream_objects(table_info)
        
        # Verify correct SQL statements were executed
        expected_calls = [
            call("USE ROLE test_role;"),
            call("USE WAREHOUSE test_warehouse;"),
            call("CREATE OR REPLACE STREAM test_cdc_db.test_cdc_schema.test_db$test_schema$test_table ON TABLE test_db.test_schema.test_table SHOW_INITIAL_ROWS = true APPEND_ONLY = FALSE"),
            call("CREATE OR REPLACE TABLE test_cdc_db.test_cdc_schema.test_db$test_schema$test_table_processing LIKE test_db.test_schema.test_table;"),
            call('ALTER TABLE test_cdc_db.test_cdc_schema.test_db$test_schema$test_table_processing ADD COLUMN IF NOT EXISTS"METADATA$ACTION" varchar;'),
            call('ALTER TABLE test_cdc_db.test_cdc_schema.test_db$test_schema$test_table_processing ADD COLUMN IF NOT EXISTS "METADATA$ISUPDATE" varchar;'),
            call('ALTER TABLE test_cdc_db.test_cdc_schema.test_db$test_schema$test_table_processing ADD COLUMN IF NOT EXISTS "METADATA$ROW_ID" varchar;')
        ]
        assert mock_cursor.execute.call_args_list == expected_calls

    @patch('snowflake.connector.connect')
    def test_create_cdc_objects_append_only(self, mock_connect, snowflake_source_warehouse):
        """Test creation of CDC objects for append-only CDC type"""
        # Set up mocks
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        snowflake_source_warehouse.config['replace_existing'] = True


        # Connect and prepare test data
        snowflake_source_warehouse.connect()
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "cdc_type": "APPEND_ONLY_STREAM"
        }
        
        # Execute the method
        snowflake_source_warehouse.create_stream_objects(table_info)
        
        # Verify correct SQL statements were executed
        expected_calls = [
            call("USE ROLE test_role;"),
            call("USE WAREHOUSE test_warehouse;"),
            call("CREATE OR REPLACE STREAM test_cdc_db.test_cdc_schema.test_db$test_schema$test_table ON TABLE test_db.test_schema.test_table SHOW_INITIAL_ROWS = true APPEND_ONLY = TRUE"),
            call("CREATE OR REPLACE TABLE test_cdc_db.test_cdc_schema.test_db$test_schema$test_table_processing LIKE test_db.test_schema.test_table;"),
            call('ALTER TABLE test_cdc_db.test_cdc_schema.test_db$test_schema$test_table_processing ADD COLUMN IF NOT EXISTS"METADATA$ACTION" varchar;'),
            call('ALTER TABLE test_cdc_db.test_cdc_schema.test_db$test_schema$test_table_processing ADD COLUMN IF NOT EXISTS "METADATA$ISUPDATE" varchar;'),
            call('ALTER TABLE test_cdc_db.test_cdc_schema.test_db$test_schema$test_table_processing ADD COLUMN IF NOT EXISTS "METADATA$ROW_ID" varchar;')
        ]
        assert mock_cursor.execute.call_args_list == expected_calls

    @patch('snowflake.connector.connect')
    def test_create_cdc_objects_no_replace(self, mock_connect, snowflake_source_warehouse):
        """Test creation of CDC objects when replace_existing is False"""
        # Set up mocks
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor
        
        # Configure warehouse to not replace existing objects
        snowflake_source_warehouse.config['replace_existing'] = False
        
        # Connect and prepare test data
        snowflake_source_warehouse.connect()
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "cdc_type": "STANDARD_STREAM"
        }
        
        # Execute the method
        snowflake_source_warehouse.create_stream_objects(table_info)
        
        # Verify correct SQL statements were executed
        expected_calls = [
            call("USE ROLE test_role;"),
            call("USE WAREHOUSE test_warehouse;"),
            call("CREATE STREAM IF NOT EXISTS test_cdc_db.test_cdc_schema.test_db$test_schema$test_table ON TABLE test_db.test_schema.test_table SHOW_INITIAL_ROWS = true APPEND_ONLY = FALSE"),
            call("CREATE TABLE test_cdc_db.test_cdc_schema.test_db$test_schema$test_table_processing IF NOT EXISTS LIKE test_db.test_schema.test_table;"),
            call('ALTER TABLE test_cdc_db.test_cdc_schema.test_db$test_schema$test_table_processing ADD COLUMN IF NOT EXISTS"METADATA$ACTION" varchar;'),
            call('ALTER TABLE test_cdc_db.test_cdc_schema.test_db$test_schema$test_table_processing ADD COLUMN IF NOT EXISTS "METADATA$ISUPDATE" varchar;'),
            call('ALTER TABLE test_cdc_db.test_cdc_schema.test_db$test_schema$test_table_processing ADD COLUMN IF NOT EXISTS "METADATA$ROW_ID" varchar;')
        ]
        assert mock_cursor.execute.call_args_list == expected_calls

    # TEST UTILITY METHODS

    @patch('snowflake.connector.connect')
    def test_execute_query(self, mock_connect, snowflake_source_warehouse):
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        snowflake_source_warehouse.connect()
        snowflake_source_warehouse.execute_query("SELECT * FROM test_table")

        # Check that execute was called with the correct arguments in the correct order
        expected_calls = [
            call(f"USE ROLE {snowflake_source_warehouse.config['role']};"),
            call(f"USE WAREHOUSE {snowflake_source_warehouse.config['warehouse']};"),
            call("SELECT * FROM test_table")
        ]
        assert mock_cursor.execute.call_args_list == expected_calls

    def test_connect_no_authenticator(self, snowflake_source_warehouse, mock_snowflake_connector):
        """Test that when no authenticator is specified, password auth is used"""
        # Arrange
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_snowflake_connector.return_value = mock_connection

        # Act
        snowflake_source_warehouse.connect()

        # Assert
        mock_snowflake_connector.assert_called_once_with(
            account="test_account",
            user="test_user",
            password="test_password"
        )

    def test_connect_externalbrowser(self, mock_snowflake_connector):
        """Test that when externalbrowser is specified, it's used for connection"""
        # Arrange
        config = {
            "account": "test_account",
            "user": "test_user",
            "role": "test_role",
            "warehouse": "test_warehouse",
            "database": "test_database",
            "change_tracking_database": "test_cdc_db",
            "change_tracking_schema": "test_cdc_schema",
            "cdc_strategy": "cdc_streams",
            "warehouse_role": "SOURCE",
            "replace_existing": False,
            "authenticator": "externalbrowser"
        }
        warehouse = SnowflakeWarehouse(config)
        
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_snowflake_connector.return_value = mock_connection

        # Act
        warehouse.connect()

        # Assert
        mock_snowflake_connector.assert_called_once_with(
            account="test_account",
            user="test_user",
            authenticator="externalbrowser"
        )

    def test_connect_invalid_authenticator(self, mock_snowflake_connector):
        """Test that specifying an invalid authenticator raises ValueError during connect"""
        # Arrange
        config = {
            "account": "test_account",
            "user": "test_user",
            "role": "test_role",
            "warehouse": "test_warehouse",
            "database": "test_database",
            "change_tracking_database": "test_cdc_db",
            "change_tracking_schema": "test_cdc_schema",
            "cdc_strategy": "cdc_streams",
            "warehouse_role": "SOURCE",
            "replace_existing": False,
            "authenticator": "invalid_auth"
        }
        warehouse = SnowflakeWarehouse(config)

        # Act/Assert
        with pytest.raises(ValueError, match="Invalid connection type.  Authenticator must be set to externalbrowser or left out."):
            warehouse.connect()

    def test_config_no_connection_file(self):
        """Test that when no connection file path is specified in config, the provided config is used as-is"""
        config = {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
            "role": "test_role",
            "warehouse": "test_warehouse",
            "database": "test_database",
            "change_tracking_database": "test_cdc_db",
            "change_tracking_schema": "test_cdc_schema",
            "cdc_strategy": "cdc_streams",
            "warehouse_role": "SOURCE",
            "replace_existing": False
        }
        
        warehouse = SnowflakeWarehouse(config)
        
        # Verify all config values match what was provided
        assert warehouse.config["account"] == "test_account"
        assert warehouse.config["user"] == "test_user"
        assert warehouse.config["password"] == "test_password"
        assert warehouse.config["role"] == "test_role"
        assert warehouse.config["warehouse"] == "test_warehouse"
        assert warehouse.config["database"] == "test_database"
        assert warehouse.config["change_tracking_database"] == "test_cdc_db"
        assert warehouse.config["change_tracking_schema"] == "test_cdc_schema"

    def test_config_with_connection_file_custom_profile(self):
        """Test that when a connection file is specified with a custom profile in config, those values override the config"""
        config = {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
            "role": "test_role",
            "warehouse": "test_warehouse",
            "database": "test_database",
            "change_tracking_database": "test_cdc_db",
            "change_tracking_schema": "test_cdc_schema",
            "cdc_strategy": "cdc_streams",
            "warehouse_role": "SOURCE",
            "replace_existing": False,
            "connection_file_path": "tests/unit/warehouses/config_files/test_snowflake_connect.toml",
            "connection_profile_name": "custom_profile"
        }
        
        warehouse = SnowflakeWarehouse(config)
        
        # Verify custom profile values overrode the initial config
        assert warehouse.config["account"] == "custom_account"
        assert warehouse.config["user"] == "custom_user"
        assert warehouse.config["password"] == "custom_password"
        assert warehouse.config["role"] == "custom_role"
        assert warehouse.config["warehouse"] == "custom_warehouse"
        assert warehouse.config["database"] == "custom_database"
        assert warehouse.config["change_tracking_database"] == "custom_cdc_db"
        assert warehouse.config["change_tracking_schema"] == "custom_cdc_schema"

    def test_config_with_connection_file_default_profile(self):
        """Test that when a connection file is specified without a profile in config, the default profile is used"""
        config = {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
            "role": "test_role",
            "warehouse": "test_warehouse",
            "database": "test_database",
            "change_tracking_database": "test_cdc_db",
            "change_tracking_schema": "test_cdc_schema",
            "cdc_strategy": "cdc_streams",
            "warehouse_role": "SOURCE",
            "replace_existing": False,
            "connection_file_path": "tests/unit/warehouses/config_files/test_snowflake_connect.toml",
        }
        
        warehouse = SnowflakeWarehouse(config)
        
        # Verify default profile values overrode the initial config
        assert warehouse.config["account"] == "default_account"
        assert warehouse.config["user"] == "default_user"
        assert warehouse.config["password"] == "default_password"
        assert warehouse.config["role"] == "default_role"
        assert warehouse.config["warehouse"] == "default_warehouse"
        assert warehouse.config["database"] == "default_database"
        assert warehouse.config["change_tracking_database"] == "default_cdc_db"
        assert warehouse.config["change_tracking_schema"] == "default_cdc_schema"

    def test_config_missing_profile(self):
        """Test that trying to use a non-existent profile raises an error"""
        config = {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
            "role": "test_role",
            "connection_file_path": "tests/unit/warehouses/config_files/test_snowflake_connect.toml",
            "connection_profile_name": "non_existent_profile"
        }
        
        with pytest.raises(ValueError, match="Profile 'non_existent_profile' not found in config file"):
            SnowflakeWarehouse(config)

    def test_config_nonexistent_toml_file(self):
        """Test that attempting to load a non-existent TOML file raises an error"""
        config = {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
            "role": "test_role",
            "connection_file_path": "nonexistent_file.toml"
        }
        
        with pytest.raises(ValueError, match="Error loading TOML configuration file:"):
            SnowflakeWarehouse(config)

    def test_config_partial_toml_override(self):
        """Test that TOML file only overrides specified values and keeps others from original config"""
        
        config = {
            "account": "test_account",
            "user": "test_user",
            "password": "test_password",
            "role": "test_role",
            "warehouse": "test_warehouse",
            "database": "test_database",
            "change_tracking_database": "test_cdc_db",
            "change_tracking_schema": "test_cdc_schema",
            "connection_file_path": "tests/unit/warehouses/config_files/test_minimal_snowflake_connect.toml"
        }
        
        warehouse = SnowflakeWarehouse(config)
        
        # Verify TOML values override original config
        assert warehouse.config["account"] == "default_account"
        assert warehouse.config["user"] == "default_user"
        # Verify non-overridden values remain from original config
        assert warehouse.config["password"] == "test_password"
        assert warehouse.config["role"] == "test_role"
        assert warehouse.config["warehouse"] == "test_warehouse"
        assert warehouse.config["database"] == "test_database"

