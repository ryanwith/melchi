# tests/config/config.py

def get_test_tables():
    test_tables = [
    {
        "schema_location": "tests/data_generators/snowflake/test_table_schemas/full_refresh_no_pk.csv",
        "table_info": {
            "database": "melchi_test_data", 
            "schema": "test_melchi_schema", 
            "table": "full_refresh_no_pk"
        }
    },
    {
        "schema_location": "tests/data_generators/snowflake/test_table_schemas/full_refresh_one_pk.csv",
        "table_info": {
            "database": "melchi_test_data", 
            "schema": "test_melchi_schema", 
            "table": "full_refresh_one_pk"
        }
    },
    {
        "schema_location": "tests/data_generators/snowflake/test_table_schemas/no_pk_standard_stream.csv",
        "table_info": {
            "database": "melchi_test_data", 
            "schema": "test_melchi_schema", 
            "table": "no_pk_standard_stream"
        }
    },
    {
        "schema_location": "tests/data_generators/snowflake/test_table_schemas/no_pk_append_only_stream.csv",
        "table_info": {
            "database": "melchi_test_data", 
            "schema": "test_melchi_schema", 
            "table": "no_pk_append_only_stream"
        }
    },
    {
        "schema_location": "tests/data_generators/snowflake/test_table_schemas/one_pk_standard_stream.csv",
        "table_info": {
            "database": "melchi_test_data", 
            "schema": "test_melchi_schema", 
            "table": "one_pk_standard_stream"
        }
    },
    {
        "schema_location": "tests/data_generators/snowflake/test_table_schemas/one_pk_append_only_stream.csv",
        "table_info": {
            "database": "melchi_test_data", 
            "schema": "test_melchi_schema", 
            "table": "one_pk_append_only_stream"
        }
    },
    {
        "schema_location": "tests/data_generators/snowflake/test_table_schemas/two_pk_standard_stream.csv",
        "table_info": {
            "database": "melchi_test_data", 
            "schema": "test_melchi_schema", 
            "table": "two_pk_standard_stream"
        }
    },
    {
        "schema_location": "tests/data_generators/snowflake/test_table_schemas/two_pk_append_only_stream.csv",
        "table_info": {
            "database": "melchi_test_data", 
            "schema": "test_melchi_schema", 
            "table": "two_pk_append_only_stream"
        }
    }
    ]
    return test_tables