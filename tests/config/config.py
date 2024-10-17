def get_test_tables():
    test_tables = [
    {
        "schema_location": "data_generators/snowflake/test_table_schemas/no_pk.csv",
        "table_info": {
            "database": "test_melchi_db", 
            "schema": "test_melchi_schema", 
            "table": "no_pk"
        }
    },
    {
        "schema_location": "data_generators/snowflake/test_table_schemas/one_pk.csv",
        "table_info": {
            "database": "test_melchi_db", 
            "schema": "test_melchi_schema", 
            "table": "one_pk"
        }
    },
    {
        "schema_location": "data_generators/snowflake/test_table_schemas/two_pk.csv",
        "table_info": {
            "database": "test_melchi_db", 
            "schema": "test_melchi_schema", 
            "table": "two_pk"
        }
    }
    ]
    return test_tables