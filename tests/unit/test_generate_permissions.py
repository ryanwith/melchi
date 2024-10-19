import unittest
from unittest.mock import patch, mock_open
from io import StringIO
from src.generate_permissions import generate_snowflake_source_permissions, write_permissions_to_file
from src.config import Config

class TestGeneratePermissions(unittest.TestCase):

    def setUp(self):
        self.mock_config = Config.from_dict({
            'source': {
                'role': 'TEST_ROLE',
                'warehouse': 'TEST_WAREHOUSE',
                'type': "snowflake",
                'change_tracking_database': 'TEST_CT_DB',
                'change_tracking_schema': 'TEST_CT_SCHEMA',
            },
            "target": {
                "type": "duckdb"
            }
        })

    @patch('src.generate_permissions.get_tables_to_transfer')
    def test_generate_snowflake_source_permissions(self, mock_get_tables):
        mock_get_tables.return_value = [
            {'database': 'DB1', 'schema': 'SCHEMA1', 'table': 'TABLE1'},
            {'database': 'DB2', 'schema': 'SCHEMA2', 'table': 'TABLE2'}
        ]

        expected_permissions = """--These grants enable Melchi to create objects that track changes
USE ROLE SECURITYADMIN;
GRANT USAGE ON WAREHOUSE TEST_WAREHOUSE TO ROLE TEST_ROLE;
GRANT USAGE ON DATABASE TEST_CT_DB TO ROLE TEST_ROLE;
GRANT USAGE ON SCHEMA TEST_CT_SCHEMA TO ROLE TEST_ROLE;
GRANT CREATE TABLE, CREATE STREAM ON SCHEMA TEST_CT_DB.TEST_CT_SCHEMA TO ROLE TEST_ROLE;
\n
--These grants enable Melchi to read changes from your objects
GRANT USAGE ON DATABASE DB1 TO ROLE TEST_ROLE;
GRANT USAGE ON DATABASE DB2 TO ROLE TEST_ROLE;
GRANT USAGE ON SCHEMA DB1.SCHEMA1 TO ROLE TEST_ROLE;
GRANT USAGE ON SCHEMA DB2.SCHEMA2 TO ROLE TEST_ROLE;
GRANT SELECT ON TABLE DB1.SCHEMA1.TABLE1 TO ROLE TEST_ROLE;
GRANT SELECT ON TABLE DB2.SCHEMA2.TABLE2 TO ROLE TEST_ROLE;
\n
--These statements alter tables to allow Melchi to create CDC streams on them
ALTER TABLE DB1.SCHEMA1.TABLE1 SET CHANGE_TRACKING = TRUE;
ALTER TABLE DB2.SCHEMA2.TABLE2 SET CHANGE_TRACKING = TRUE;
"""

        permissions = generate_snowflake_source_permissions(self.mock_config)
        self.assertEqual(permissions.strip(), expected_permissions.strip())


    def test_write_permissions_to_file(self):
        test_permissions = "TEST PERMISSION 1;\nTEST PERMISSION 2;"
        mock_file = mock_open()

        with patch('builtins.open', mock_file):
            write_permissions_to_file(test_permissions, "test_output.sql")

        mock_file.assert_called_once_with("test_output.sql", "w")
        handle = mock_file()
        handle.write.assert_called_once_with(test_permissions)

if __name__ == '__main__':
    unittest.main()