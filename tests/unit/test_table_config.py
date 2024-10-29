import pytest
import os
import csv
from src.utils.table_config import get_tables_to_transfer
from src.config import Config
from unittest.mock import Mock, patch

@pytest.fixture
def mock_config():
    config = Mock(spec=Config)
    config.get_tables_config_path.return_value = "test_tables.csv"
    return config

@pytest.fixture
def temp_csv(tmp_path):
    """Creates a temporary CSV file and returns its path."""
    csv_path = tmp_path / "test_tables.csv"
    return str(csv_path)

def write_test_csv(file_path, content):
    """Helper function to write test CSV content."""
    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(content)

def test_valid_cdc_types(tmp_path, mock_config):
    # Create test CSV with different CDC types
    csv_content = [
        ['database', 'schema', 'table', 'cdc_type'],
        ['db1', 'sch1', 'tbl1', 'APPEND_ONLY_STREAM'],
        ['db2', 'sch2', 'tbl2', 'STANDARD_STREAM'],
        ['db3', 'sch3', 'tbl3', ''],  # Test default value
        ['db4', 'sch4', 'tbl4', 'append_only_stream'],  # Test lowercase
        ['db5', 'sch5', 'tbl5', 'standard_stream'],  # Test lowercase
        ['db6', 'sch6', 'tbl6', 'full_refresh'],  # Test lowercase
        ['db7', 'sch7', 'tbl7', 'FULL_REFRESH'],  # Test uppercase
    ]
    
    csv_path = tmp_path / "test_tables.csv"
    write_test_csv(csv_path, csv_content)
    
    with patch('os.path.join', return_value=str(csv_path)):
        result = get_tables_to_transfer(mock_config)
        
        assert len(result) == 7
        assert result[0] == {'database': 'db1', 'schema': 'sch1', 'table': 'tbl1', 'cdc_type': 'APPEND_ONLY_STREAM'}
        assert result[1] == {'database': 'db2', 'schema': 'sch2', 'table': 'tbl2', 'cdc_type': 'STANDARD_STREAM'}
        assert result[2] == {'database': 'db3', 'schema': 'sch3', 'table': 'tbl3', 'cdc_type': 'FULL_REFRESH'}
        assert result[3] == {'database': 'db4', 'schema': 'sch4', 'table': 'tbl4', 'cdc_type': 'APPEND_ONLY_STREAM'}
        assert result[4] == {'database': 'db5', 'schema': 'sch5', 'table': 'tbl5', 'cdc_type': 'STANDARD_STREAM'}
        assert result[5] == {'database': 'db6', 'schema': 'sch6', 'table': 'tbl6', 'cdc_type': 'FULL_REFRESH'}
        assert result[6] == {'database': 'db7', 'schema': 'sch7', 'table': 'tbl7', 'cdc_type': 'FULL_REFRESH'}

def test_invalid_file_path(mock_config):
    with patch('os.path.join', return_value='nonexistent.csv'):
        result = get_tables_to_transfer(mock_config)
        assert result == []

def test_invalid_cdc_type(tmp_path, mock_config):
    # Create test CSV with invalid CDC type
    csv_content = [
        ['database', 'schema', 'table', 'cdc_type'],
        ['db1', 'sch1', 'tbl1', 'INVALID_TYPE']
    ]
    
    csv_path = tmp_path / "test_tables.csv"
    write_test_csv(csv_path, csv_content)
    
    with patch('os.path.join', return_value=str(csv_path)):
        with pytest.raises(ValueError) as exc_info:
            get_tables_to_transfer(mock_config)
        
        assert str(exc_info.value) == "INVALID_TYPE is not a valid CDC type.  Please provide FULL_REFRESH, STANDARD_STREAM, or APPEND_ONLY_STREAM, or leave it blank to default to FULL_REFRESH."

def test_no_cdc_column(tmp_path, mock_config):
    # Create test CSV with invalid CDC type
    csv_content = [
        ['database', 'schema', 'table'],
        ['db1', 'sch1', 'tbl1']
    ]

    csv_path = tmp_path / "test_tables.csv"
    write_test_csv(csv_path, csv_content)
    
    # tables = get_tables_to_transfer(mock_config)
    with patch('os.path.join', return_value=str(csv_path)):
        tables = get_tables_to_transfer(mock_config)
        assert tables[0] =={'database': 'db1', 'schema': 'sch1', 'table': 'tbl1', 'cdc_type': 'FULL_REFRESH'}
        
def test_no_cdc_specified(tmp_path, mock_config):
    # Create test CSV with invalid CDC type
    csv_content = [
        ['database', 'schema', 'table', "cdc_type"],
        ['db1', 'sch1', 'tbl1']
    ]

    csv_path = tmp_path / "test_tables.csv"
    write_test_csv(csv_path, csv_content)
    
    # tables = get_tables_to_transfer(mock_config)
    with patch('os.path.join', return_value=str(csv_path)):
        tables = get_tables_to_transfer(mock_config)
        assert tables[0] =={'database': 'db1', 'schema': 'sch1', 'table': 'tbl1', 'cdc_type': 'FULL_REFRESH'}
        

def test_utf8_bom_handling(tmp_path, mock_config):
    """Test handling of UTF-8 BOM in CSV file"""
    csv_path = tmp_path / "test_tables.csv"
    
    # Write CSV with UTF-8 BOM
    with open(csv_path, 'wb') as f:
        f.write(b'\xef\xbb\xbf')  # UTF-8 BOM
        f.write(b'database,schema,table,cdc_type\n')
        f.write(b'db1,sch1,tbl1,STANDARD_STREAM\n')
    
    with patch('os.path.join', return_value=str(csv_path)):
        result = get_tables_to_transfer(mock_config)
        
        assert len(result) == 1
        assert result[0] == {'database': 'db1', 'schema': 'sch1', 'table': 'tbl1', 'cdc_type': 'STANDARD_STREAM'}

def test_malformed_csv(tmp_path):
    # Create test cases for malformed CSV content
    test_cases = [
        # Missing required field
        "database,schema,table,cdc_type\n,,table1,STANDARD_STREAM",
        "database,schema,table,cdc_type\ndb1,,table1,STANDARD_STREAM",
        "database,schema,table,cdc_type\ndb1,schema1,,STANDARD_STREAM",
        
        # Invalid CDC type
        "database,schema,table,cdc_type\ndb1,schema1,table1,INVALID_TYPE",
    ]

    config = Mock()
    
    for i, content in enumerate(test_cases):
        # Create temporary CSV file
        csv_path = tmp_path / f"test_tables_{i}.csv"
        csv_path.write_text(content)
        config.get_tables_config_path.return_value = str(csv_path)
        with pytest.raises(ValueError):
            get_tables_to_transfer(config)

def test_invalid_file_path(mock_config):
    with patch('os.path.join', return_value='nonexistent.csv'):
        with pytest.raises(FileNotFoundError) as exc_info:
            get_tables_to_transfer(mock_config)
        assert str(exc_info.value) == "Configuration file not found at nonexistent.csv"