import pytest
from unittest.mock import Mock, patch, call
from src.warehouses.snowflake_warehouse import SnowflakeWarehouse

class TestSnowflakeWarehouse:
    @pytest.fixture
    def mock_snowflake_connector(self):
        with patch('snowflake.connector.connect') as mock_connect:
            yield mock_connect

    @pytest.fixture
    def snowflake_warehouse(self):
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
            "warehouse_role": "SOURCE"
        }
        return SnowflakeWarehouse(config)

    # Connection tests
    def test_connect(self, snowflake_warehouse, mock_snowflake_connector):
        # Arrange
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_snowflake_connector.return_value = mock_connection

        # Act
        snowflake_warehouse.connect()

        # Assert
        mock_snowflake_connector.assert_called_once_with(
            account="test_account",
            user="test_user",
            password="test_password",
            # Add other config parameters as needed
        )
        assert snowflake_warehouse.connection == mock_connection
        assert snowflake_warehouse.cursor == mock_cursor
        mock_cursor.execute.assert_any_call("USE ROLE test_role;")
        mock_cursor.execute.assert_any_call("USE WAREHOUSE test_warehouse;")

    def test_disconnect(self, snowflake_warehouse, mock_snowflake_connector):
        # Arrange
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_snowflake_connector.return_value = mock_connection
        snowflake_warehouse.connect()

        # Act
        snowflake_warehouse.disconnect()

        # Assert
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
        assert snowflake_warehouse.cursor is None
        assert snowflake_warehouse.connection is None

    def test_connect_error(self, snowflake_warehouse, mock_snowflake_connector):
        # Arrange
        mock_snowflake_connector.side_effect = Exception("Connection failed")

        # Act & Assert
        with pytest.raises(Exception, match="Connection failed"):
            snowflake_warehouse.connect()

    def test_disconnect_when_not_connected(self, snowflake_warehouse):
        # Arrange
        snowflake_warehouse.cursor = None
        snowflake_warehouse.connection = None

        # Act
        snowflake_warehouse.disconnect()

        # Assert
        # This test passes if no exception is raised

    # Transaction management tests
    @patch('snowflake.connector.connect')
    def test_begin_and_commit_transaction(self, mock_connect, snowflake_warehouse):
        mock_cursor = Mock()
        mock_connect.return_value.cursor.return_value = mock_cursor
        
        snowflake_warehouse.connect()
        snowflake_warehouse.begin_transaction()
        snowflake_warehouse.commit_transaction()
        
        mock_cursor.execute.assert_has_calls([call("BEGIN;")])
        mock_connect.return_value.commit.assert_called_once()

    @patch('snowflake.connector.connect')
    def test_rollback_transaction(self, mock_connect, snowflake_warehouse):
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        snowflake_warehouse.connect()
        snowflake_warehouse.rollback_transaction()
        
        mock_connection.rollback.assert_called_once()

    # Schema and table management tests

    @patch('snowflake.connector.connect')
    def test_get_schema(self, mock_connect, snowflake_warehouse):
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
        snowflake_warehouse.connect()

        # Call get_schema
        table_info = {"database": "test_db", "schema": "test_schema", "table": "test_table"}
        schema = snowflake_warehouse.get_schema(table_info)

        # Assertions
        mock_connect.assert_called_once_with(
            account=snowflake_warehouse.config['account'],
            user=snowflake_warehouse.config['user'],
            password=snowflake_warehouse.config['password']
        )
        mock_cursor.execute.assert_called_with(f"DESC TABLE {snowflake_warehouse.get_full_table_name(table_info)}")
        
        assert len(schema) == 2
        assert schema[0] == {
            "name": "column1", "type": "VARCHAR", "nullable": True, 
            "default_value": None, "primary_key": False
        }
        assert schema[1] == {
            "name": "column2", "type": "INTEGER", "nullable": False, 
            "default_value": None, "primary_key": True
        }

    def test_get_full_table_name(self, snowflake_warehouse):
        """Test table name formatting"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table"
        }
        assert snowflake_warehouse.get_full_table_name(table_info) == "test_db.test_schema.test_table"

    def test_replace_existing_tables(self, snowflake_warehouse):
        """Test table replacement configuration"""
        # Test with default config
        assert snowflake_warehouse.replace_existing_tables() == False
        
        # Test with explicit config
        snowflake_warehouse.config["replace_existing"] = True
        assert snowflake_warehouse.replace_existing_tables() == True

    # Change Tracking Tests
    def test_get_change_tracking_schema_full_name(self, snowflake_warehouse):
        expected = f"{snowflake_warehouse.config['change_tracking_database']}.{snowflake_warehouse.config['change_tracking_schema']}"
        assert snowflake_warehouse.get_change_tracking_schema_full_name() == expected

    def test_get_stream_name(self, snowflake_warehouse):
        """Test stream name generation follows expected pattern"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema", 
            "table": "test_table"
        }
        expected = f"{snowflake_warehouse.get_change_tracking_schema_full_name()}.test_db$test_schema$test_table"
        assert snowflake_warehouse.get_stream_name(table_info) == expected

    def test_get_stream_processing_table_name(self, snowflake_warehouse):
        """Test processing table name generation follows expected pattern"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table"
        }
        expected = f"{snowflake_warehouse.get_change_tracking_schema_full_name()}.test_db$test_schema$test_table_processing"
        assert snowflake_warehouse.get_stream_processing_table_name(table_info) == expected

    def test_setup_environment_no_tables(self, snowflake_warehouse):
        """Test environment setup fails without tables"""
        with pytest.raises(Exception, match="No tables to transfer found"):
            snowflake_warehouse.setup_environment([])

    def test_setup_environment_invalid_strategy(self, snowflake_warehouse):
        """Test invalid CDC strategy is caught"""
        snowflake_warehouse.config["cdc_strategy"] = "invalid_strategy"
        with pytest.raises(ValueError, match="Invalid or no cdc_strategy provided"):
            snowflake_warehouse.setup_environment([{"database": "db", "schema": "sch", "table": "tbl"}])

    # TEST UTILITY METHODS

    @patch('snowflake.connector.connect')
    def test_execute_query(self, mock_connect, snowflake_warehouse):
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connect.return_value = mock_connection
        mock_connection.cursor.return_value = mock_cursor

        snowflake_warehouse.connect()
        snowflake_warehouse.execute_query("SELECT * FROM test_table")

        # Check that execute was called with the correct arguments in the correct order
        expected_calls = [
            call(f"USE ROLE {snowflake_warehouse.config['role']};"),
            call(f"USE WAREHOUSE {snowflake_warehouse.config['warehouse']};"),
            call("SELECT * FROM test_table")
        ]
        assert mock_cursor.execute.call_args_list == expected_calls