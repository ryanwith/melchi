from util.helpers import execute_array_of_queries, snowflake_context_queries

def snowflake_setup_all_cdc(cursor, table_sets):
    print("Running function snowflake_setup_all_cdc")
    for table_set in table_sets:
        snowflake_setup_cdc_for_table_set(cursor, table_set)

def snowflake_setup_cdc_for_table_set(cursor, table_set):
    print("Running function snowflake_setup_cdc_for_table_set")
    context_queries = snowflake_context_queries(table_set["role"], table_set["database"], table_set["warehouse"])
    execute_array_of_queries(cursor, context_queries)
    for table in table_set["tables_to_replicate"]:
        snowflake_setup_cdc_for_table(cursor, table, table_set["cdc_schema"])

def snowflake_setup_cdc_for_table(cursor, table_name: str, cdc_schema: str = 'melchi', replace_existing: bool = False):
    print(f"Running function snowflake_setup_cdc_for_table for table {table_name}")
    raw_table_name = table_name.rsplit('.', 1)[-1] if '.' in table_name else table_name
    stream_name = f"{raw_table_name}_stream"
    persisted_stream_name = f"{raw_table_name}_persisted_stream"

    # Step 1: Create streams
    if replace_existing != True:
        create_stream_query = f"CREATE STREAM {cdc_schema}.{stream_name} ON TABLE {table_name} SHOW_INITIAL_ROWS = true;"
    else:
        create_stream_query = f"CREATE OR REPLACE STREAM {cdc_schema}.{stream_name} ON TABLE {table_name} SHOW_INITIAL_ROWS = true;"
        
    # Step 2: Create tables we will persist stream data into
    if replace_existing != True:
        create_persisted_stream_query = f"""
        CREATE TABLE {cdc_schema}.{persisted_stream_name} AS 
            SELECT * FROM {cdc_schema}.{stream_name};"""
    else:
        create_persisted_stream_query = f"""
        CREATE OR REPLACE TABLE {cdc_schema}.{persisted_stream_name} AS 
            SELECT * FROM {cdc_schema}.{stream_name};"""

    # Step 3: Alter `persisted_stream_name` table to add `record_timestamp` column if not exists
    alter_table_add_timestamps_column_query = f"ALTER TABLE {cdc_schema}.{persisted_stream_name} ADD COLUMN IF NOT EXISTS record_timestamp TIMESTAMP;"

    # Step 4: Update the persisted_stream_name table to set current_timestamp for NULL values in `record_timestamp` in UTC
    add_timestamps_query = f"""
        UPDATE {cdc_schema}.{persisted_stream_name}
        SET record_timestamp = sysdate()
        WHERE record_timestamp IS NULL;
        """

    setup_queries_array = [
        "BEGIN;",
        create_stream_query,
        create_persisted_stream_query, 
        alter_table_add_timestamps_column_query,
        add_timestamps_query,
        "COMMIT;"
    ]

    execute_array_of_queries(cursor, setup_queries_array)

def snowflake_run_cdc(cursor, table_sets):
    print("Running function snowflake_setup_all_cdc")
    for table_set in table_sets:
        snowflake_run_cdc_for_table_set(cursor, table_set)

def snowflake_run_cdc_for_table_set(cursor, table_set):
    print("Running function snowflake_setup_cdc_for_table_set")
    context_queries = snowflake_context_queries(table_set["role"], table_set["database"], table_set["warehouse"])
    execute_array_of_queries(cursor, context_queries)
    for table in table_set["tables_to_replicate"]:
        snowflake_get_table_changes(cursor, table, table_set["cdc_schema"])

def snowflake_get_table_changes(cursor, table_name: str, cdc_schema: str = 'melchi'):
    raw_table_name = table_name.rsplit('.', 1)[-1] if '.' in table_name else table_name
    stream_name = f"{raw_table_name}_stream"
    persisted_stream_name = f"{raw_table_name}_persisted_stream"
    temp_table_name = f"{cdc_schema}.{persisted_stream_name}_staging"
    create_temp_table_query = f"""
        CREATE OR REPLACE TEMP TABLE {temp_table_name} AS
        SELECT * FROM {cdc_schema}.{stream_name};
    """
    alter_table_add_timestamps_column_query = f"ALTER TABLE {temp_table_name} ADD COLUMN IF NOT EXISTS record_timestamp TIMESTAMP;"
    add_timestamps_query = f"""
        UPDATE {temp_table_name}
        SET record_timestamp = sysdate
        WHERE record_timestamp IS NULL;
        """

    insert_changes_query = f"""
        INSERT INTO {cdc_schema}.{persisted_stream_name}
        SELECT * FROM {temp_table_name}
        """
    
    drop_temp_table_query = f"DROP TABLE {temp_table_name}"
    cdc_queries = [
        "BEGIN;",
        create_temp_table_query,
        alter_table_add_timestamps_column_query,
        insert_changes_query,
        drop_temp_table_query,
        "COMMIT;"
    ]

    execute_array_of_queries(cursor, cdc_queries)
