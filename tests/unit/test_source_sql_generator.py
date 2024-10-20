import unittest
from unittest.mock import patch, mock_open
from io import StringIO
from src.source_sql_generator import generate_snowflake_source_sql, write_sql_to_file
from src.config import Config

class TestSourceSQLGenerator(unittest.TestCase):

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

    @patch('src.source_sql_generator.get_tables_to_transfer')
    def test_generate_snowflake_source_permissions(self, mock_get_tables):
        mock_get_tables.return_value = [
            {'database': 'DB1', 'schema': 'SCHEMA1', 'table': 'TABLE1'},
            {'database': 'DB2', 'schema': 'SCHEMA2', 'table': 'TABLE2'}
        ]

        expected_permissions = """--This statement uses to a role that should have permissions to perform the following actions.  You may need to use one or more other roles if you do not have access to SECURITYADMIN.
USE ROLE SECURITYADMIN;
\n
--This command creates the change tracking schema.  Not required if it already exists.
CREATE SCHEMA IF NOT EXISTS TEST_CT_DB.TEST_CT_SCHEMA;
\n
--These statements enable Melchi to create streams that track changes on the provided tables.
ALTER TABLE DB1.SCHEMA1.TABLE1 SET CHANGE_TRACKING = TRUE;
ALTER TABLE DB2.SCHEMA2.TABLE2 SET CHANGE_TRACKING = TRUE;
\n
--These grants enable Melchi to create objects that track changes.
GRANT USAGE ON WAREHOUSE TEST_WAREHOUSE TO ROLE TEST_ROLE;
GRANT USAGE ON DATABASE TEST_CT_DB TO ROLE TEST_ROLE;
GRANT USAGE ON SCHEMA TEST_CT_SCHEMA TO ROLE TEST_ROLE;
GRANT CREATE TABLE, CREATE STREAM ON SCHEMA TEST_CT_DB.TEST_CT_SCHEMA TO ROLE TEST_ROLE;
\n
--These grants enable Melchi to read changes from your objects.
GRANT USAGE ON DATABASE DB1 TO ROLE TEST_ROLE;
GRANT USAGE ON DATABASE DB2 TO ROLE TEST_ROLE;
GRANT USAGE ON SCHEMA DB1.SCHEMA1 TO ROLE TEST_ROLE;
GRANT USAGE ON SCHEMA DB2.SCHEMA2 TO ROLE TEST_ROLE;
GRANT SELECT ON TABLE DB1.SCHEMA1.TABLE1 TO ROLE TEST_ROLE;
GRANT SELECT ON TABLE DB2.SCHEMA2.TABLE2 TO ROLE TEST_ROLE;
"""

        permissions = generate_snowflake_source_sql(self.mock_config)
        print("\n")
        print(permissions)
        print("\n\n\n")
        print(expected_permissions)
        self.assertEqual(permissions.strip(), expected_permissions.strip())


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