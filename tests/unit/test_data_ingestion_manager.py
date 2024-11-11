import pytest
from unittest.mock import Mock, patch, call, ANY
from src.data_ingestion_manager import DataIngestionManager
from uuid import UUID
from tests.utils.helpers import normalize_sql

@pytest.fixture
def mock_source_warehouse():
    mock = Mock()
    mock.warehouse_type = "snowflake"
    mock.get_supported_cdc_types.return_value = ("STANDARD_STREAM", "APPEND_ONLY_STREAM", "FULL_REFRESH")
    return mock

@pytest.fixture
def mock_target_warehouse():
    mock = Mock()
    mock.warehouse_type = "duckdb"
    return mock

@pytest.fixture
def manager(mock_source_warehouse, mock_target_warehouse):
    return DataIngestionManager(mock_source_warehouse, mock_target_warehouse)

class TestDataIngestionManager:
    def test_initialization(self, manager, mock_source_warehouse, mock_target_warehouse):
        """Test proper initialization of DataIngestionManager"""
        assert manager.source_warehouse == mock_source_warehouse
        assert manager.target_warehouse == mock_target_warehouse

    def test_sync_table_unsupported_cdc_type(self, manager):
        """Test that sync_table raises error for unsupported CDC types"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "cdc_type": "UNSUPPORTED_TYPE"
        }
        
        with pytest.raises(ValueError) as exc_info:
            manager.sync_table(table_info)
        
        # assert "Invalid cdc_type" in str(exc_info.value)
        assert "UNSUPPORTED_TYPE is not a valid CDC type" in str(exc_info.value)

    def test_sync_table_full_refresh(self, manager):
        """Test that sync_table routes to _handle_full_refresh for FULL_REFRESH type"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "cdc_type": "FULL_REFRESH"
        }
        
        with patch.object(manager, '_handle_full_refresh') as mock_handle_full_refresh:
            manager.sync_table(table_info)
            mock_handle_full_refresh.assert_called_once_with(table_info)

    def test_sync_table_standard_stream(self, manager):
        """Test that sync_table routes to _handle_stream_updates for STANDARD_STREAM type"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "cdc_type": "STANDARD_STREAM"
        }
        
        with patch.object(manager, '_handle_stream_updates') as mock_handle_stream:
            manager.sync_table(table_info)
            mock_handle_stream.assert_called_once_with(table_info)

    def test_sync_table_append_only_stream(self, manager):
        """Test that sync_table routes to _handle_stream_updates for APPEND_ONLY_STREAM type"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "cdc_type": "APPEND_ONLY_STREAM"
        }
        
        with patch.object(manager, '_handle_stream_updates') as mock_handle_stream:
            manager.sync_table(table_info)
            mock_handle_stream.assert_called_once_with(table_info)

    def test_sync_table_cdc_type_case_insensitive(self, manager):
        """Test that sync_table handles CDC types case-insensitively"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "cdc_type": "full_refresh"  # lowercase
        }
        
        with patch.object(manager, '_handle_full_refresh') as mock_handle_full_refresh:
            manager.sync_table(table_info)
            mock_handle_full_refresh.assert_called_once_with(table_info)

    def test_handle_full_refresh_success(self, manager):
        """Test successful full refresh operation"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "cdc_type": "FULL_REFRESH"
        }

        # Mock successful batch processing
        mock_df_batches = [Mock(), Mock()]  # Two mock dataframes
        manager.source_warehouse.get_batches_for_full_refresh.return_value = mock_df_batches

        # Execute the full refresh
        manager._handle_full_refresh(table_info)

        # Verify sequence of operations
        assert manager.target_warehouse.connect.called
        assert manager.source_warehouse.connect.called
        assert manager.target_warehouse.begin_transaction.called
        assert manager.target_warehouse.truncate_table.called
        assert manager.target_warehouse.process_insert_batches.called
        assert manager.target_warehouse.update_cdc_trackers.called
        assert manager.target_warehouse.commit_transaction.called
        assert manager.target_warehouse.disconnect.called
        assert manager.source_warehouse.disconnect.called

        # Verify order of operations
        manager.target_warehouse.method_calls == [
            call.connect(),
            call.begin_transaction(),
            call.truncate_table(table_info),
            call.process_insert_batches(table_info, mock_df_batches, ANY),
            call.update_cdc_trackers(table_info, ANY),
            call.commit_transaction(),
            call.disconnect()
        ]

    def test_handle_full_refresh_error(self, manager):
        """Test error handling during full refresh"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "cdc_type": "FULL_REFRESH"
        }

        # Simulate an error during processing
        manager.target_warehouse.process_insert_batches.side_effect = Exception("Processing error")

        # Execute and verify error handling
        manager._handle_full_refresh(table_info)

        # Verify rollback and cleanup
        assert manager.target_warehouse.rollback_transaction.called
        assert manager.target_warehouse.disconnect.called
        assert manager.source_warehouse.disconnect.called

    def test_handle_stream_updates_standard_stream(self, manager):
        """Test successful stream updates for standard stream"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "cdc_type": "STANDARD_STREAM"
        }

        # Mock successful batch processing
        mock_delete_batches = [Mock()]
        mock_insert_batches = [Mock()]
        manager.source_warehouse.get_delete_batches_for_stream.return_value = mock_delete_batches
        manager.source_warehouse.get_insert_batches_for_stream.return_value = mock_insert_batches
        manager.target_warehouse.get_etl_ids.return_value = []

        # Execute stream updates
        manager._handle_stream_updates(table_info)

        # Verify sequence and order of operations
        assert manager.target_warehouse.connect.called
        assert manager.source_warehouse.connect.called
        assert manager.target_warehouse.begin_transaction.called
        assert manager.source_warehouse.begin_transaction.called
        assert manager.source_warehouse.prepare_stream_ingestion.called
        assert manager.target_warehouse.process_delete_batches.called
        assert manager.target_warehouse.process_insert_batches.called
        assert manager.target_warehouse.update_cdc_trackers.called
        assert manager.target_warehouse.commit_transaction.called
        assert manager.source_warehouse.cleanup_source.called
        assert manager.source_warehouse.commit_transaction.called
        assert manager.source_warehouse.disconnect.called
        assert manager.target_warehouse.disconnect.called

    def test_handle_stream_updates_append_only(self, manager):
        """Test successful stream updates for append-only stream"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "cdc_type": "APPEND_ONLY_STREAM"
        }

        # Mock successful batch processing
        mock_insert_batches = [Mock()]
        manager.source_warehouse.get_insert_batches_for_stream.return_value = mock_insert_batches
        manager.target_warehouse.get_etl_ids.return_value = []

        # Execute stream updates
        manager._handle_stream_updates(table_info)

        # Verify sequence and order of operations
        assert manager.target_warehouse.connect.called
        assert manager.source_warehouse.connect.called
        assert manager.target_warehouse.begin_transaction.called
        assert manager.source_warehouse.begin_transaction.called
        assert manager.source_warehouse.prepare_stream_ingestion.called
        assert not manager.target_warehouse.process_delete_batches.called  # Should not process deletes
        assert manager.target_warehouse.process_insert_batches.called
        assert manager.target_warehouse.update_cdc_trackers.called
        assert manager.target_warehouse.commit_transaction.called
        assert manager.source_warehouse.cleanup_source.called
        assert manager.source_warehouse.commit_transaction.called
        assert manager.source_warehouse.disconnect.called
        assert manager.target_warehouse.disconnect.called

    def test_handle_stream_updates_error(self, manager):
        """Test error handling during stream updates"""
        table_info = {
            "database": "test_db",
            "schema": "test_schema",
            "table": "test_table",
            "cdc_type": "STANDARD_STREAM"
        }

        # Simulate an error during processing
        manager.target_warehouse.process_insert_batches.side_effect = Exception("Processing error")
        manager.target_warehouse.get_etl_ids.return_value = []

        # Execute stream updates
        manager._handle_stream_updates(table_info)

        # Verify rollback and cleanup for both warehouses
        assert manager.target_warehouse.rollback_transaction.called
        assert manager.source_warehouse.rollback_transaction.called
        assert manager.source_warehouse.disconnect.called
        assert manager.target_warehouse.disconnect.called