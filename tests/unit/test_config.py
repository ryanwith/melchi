import unittest
from unittest.mock import patch, mock_open
from src.config import Config

class TestConfig(unittest.TestCase):

    def test_load_config_from_yaml(self):
        yaml_content = """
        source:
          type: snowflake
          account: ${SNOWFLAKE_ACCOUNT}
        target:
          type: duckdb
          database: ${DUCKDB_DATABASE}
        """
        with patch('builtins.open', mock_open(read_data=yaml_content)):
            with patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test_account', 'DUCKDB_DATABASE': '/path/to/db'}):
                config = Config(config_path='dummy_path.yaml')
                self.assertEqual(config.source_type, 'snowflake')
                self.assertEqual(config.source_config['account'], 'test_account')
                self.assertEqual(config.target_type, 'duckdb')
                self.assertEqual(config.target_config['database'], '/path/to/db')

    def test_config_from_dict(self):
        config_dict = {
            'source': {'type': 'snowflake', 'account': 'test_account'},
            'target': {'type': 'duckdb', 'database': '/path/to/db'}
        }
        config = Config.from_dict(config_dict)
        self.assertEqual(config.source_type, 'snowflake')
        self.assertEqual(config.source_config['account'], 'test_account')
        self.assertEqual(config.target_type, 'duckdb')
        self.assertEqual(config.target_config['database'], '/path/to/db')

    def test_get_tables_config_path(self):
        config_dict = {
            'source': {'type': 'snowflake'},
            'target': {'type': 'duckdb'},
            'tables_config': {'path': 'path/to/tables.csv'}
        }
        config = Config.from_dict(config_dict)
        self.assertEqual(config.get_tables_config_path(), 'path/to/tables.csv')

    def test_missing_required_config(self):
        with self.assertRaises(ValueError):
            Config()

if __name__ == '__main__':
    unittest.main()