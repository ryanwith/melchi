from snowflake.connector import ProgrammingError

def execute_array_of_queries(cursor, queries):
    print("Begin executing queries")
    try:
        for query in queries:
            print(f"Executing {query}")
            cursor.execute(query)
            print("Executed")
    except ProgrammingError as e:
        print(f"Failed to execute query: {query}")
        print(f"Error: {str(e)}")
    finally:
        print("Finished executing queries.")

def snowflake_context_queries(role: str, database: str, warehouse: str):
    return [ 
        f"USE ROLE {role};",  # Use the specified role
        f"USE WAREHOUSE {warehouse};",  # Use the specified warehouse
        f"USE DATABASE {database};"  # Use the specified database
    ]