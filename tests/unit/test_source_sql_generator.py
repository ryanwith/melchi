# tests/unit/test_source_sql_generator.py

import unittest
from unittest.mock import patch, mock_open
from io import StringIO
from src.source_sql_generator import write_sql_to_file
from src.config import Config

class TestSourceSQLGenerator(unittest.TestCase):

    

    def test_write_sql_to_file(self):
        test_permissions = "TEST PERMISSION 1;\nTEST PERMISSION 2;"
        mock_file = mock_open()

        with patch('builtins.open', mock_file):
            write_sql_to_file(test_permissions, "test_output.sql")

        mock_file.assert_called_once_with("test_output.sql", "w")
        handle = mock_file()
        handle.write.assert_called_once_with(test_permissions)

if __name__ == '__main__':
    unittest.main()