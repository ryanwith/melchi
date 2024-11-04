# tests/data_generators/snowflake/snowflake_data_generator.py

import random
import numpy as np
from datetime import datetime, date, time
import json
from shapely.geometry import Point
from decimal import Decimal

# Set random seed for reproducibility
np.random.seed(42)

# Generate 50 rows of data

# Helper function to generate random timestamps
def random_timestamp():
    return datetime.fromtimestamp(np.random.randint(1577836800, 1893456000))

def large_random_int():
    return np.random.randint(-9999999999999999, 9999999999999999) * 10**19 + np.random.randint(-9999999999999999, 9999999999999999)

def large_random_with_decimal(max_integer_digits, max_decimal_places):
    integer_part = random.randint(0 - 10 ** max_integer_digits, 10 ** max_integer_digits)
    decimal_part = random.randint(0, 10 ** max_decimal_places)
    return Decimal(f"{integer_part}.{decimal_part}")


# Helper function to format values for SQL INSERT
def format_value(value):
    if value is None:
        return 'NULL'
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, bool):
        return str(value).upper()
    elif isinstance(value, (datetime, date, time)):
        return f"'{value}'"
    elif isinstance(value, bytes):
        return f"X'{value.hex()}'"
    elif isinstance(value, (dict, list)):
        return f"PARSE_JSON('{json.dumps(value)}')"
    elif isinstance(value, str):
        if value.startswith("VECTOR_TO_FOLLOW"):
            return value.split("::::")[1]
        else:
            return f"'{value}'"
    elif isinstance(value, Decimal):
        return str(value)
    else:
        return f"'{value}'"
    
def generate_snowflake_data(num_rows, include_geo_columns = False):
    def safe_large_float():
        sign = 1 if np.random.random() > 0.5 else -1
        exponent = np.random.randint(0, 308)
        mantissa = np.random.random()
        return sign * mantissa * (10 ** exponent)

    data = []
    for i in range(num_rows):
        row = [
            large_random_int(),  # NUMBER(38,0)
            large_random_with_decimal(28,10),  # DECIMAL
            large_random_with_decimal(27,11),  # DECIMAL
            large_random_with_decimal(38,0),  # NUMERIC
            np.random.randint(-2147483648, 2147483647),  # INT
            np.random.randint(-2147483648, 2147483647),  # INTEGER
            np.random.randint(-9223372036854775808, 9223372036854775807),  # BIGINT
            np.random.randint(-32768, 32767),  # SMALLINT
            np.random.randint(-128, 127),  # TINYINT
            np.random.randint(-128, 127),  # BYTEINT
            safe_large_float(),  # FLOAT
            safe_large_float(),  # FLOAT4 (we'll use the same function for simplicity)
            safe_large_float(),  # FLOAT8
            safe_large_float(),  # DOUBLE
            safe_large_float(),  # DOUBLEPRECISION
            safe_large_float(),  # REAL
            f'varchar_{i}',  # VARCHAR
            chr(np.random.randint(65, 91)),  # CHAR
            chr(np.random.randint(65, 91)),  # CHARACTER
            f'string_{i}',  # STRING
            f'This is a longer text for row {i}.',  # TEXT
            np.random.bytes(10),  # BINARY
            np.random.bytes(np.random.randint(1, 20)),  # VARBINARY
            np.random.choice([True, False]),  # BOOLEAN
            date(np.random.randint(1, 9999), np.random.randint(1, 13), np.random.randint(1, 29)),  # DATE
            random_timestamp(),  # DATETIME
            time(np.random.randint(0, 24), np.random.randint(0, 60), np.random.randint(0, 60)),  # TIME
            random_timestamp(),  # TIMESTAMP
            random_timestamp(),  # TIMESTAMP_LTZ
            random_timestamp(),  # TIMESTAMP_NTZ
            random_timestamp(),  # TIMESTAMP_TZ
            {'key': f'value_{i}'},  # VARIANT
            {'nested': {'key': f'value_{i}'}},  # OBJECT
            [i, i+1, i+2],  # ARRAY
            f"VECTOR_TO_FOLLOW::::{np.random.rand(256).tolist()}::VECTOR(FLOAT, 256)"  # VECTOR_FLOAT_256
        ]
        if include_geo_columns:
            row.append(Point(np.random.uniform(-180, 180), np.random.uniform(-90, 90)).wkt)  # GEOGRAPHY
            row.append(Point(np.random.uniform(-180, 180), np.random.uniform(-90, 90)).wkt)  # GEOMETRY
        data.append(row)
    
    return data

def generate_insert_statement(table_name, data):
    values_str = ',\n    '.join([
        '(' + ', '.join(format_value(value) for value in row) + ')'
        for row in data
    ])
    insert_statement = f"""
    INSERT INTO {table_name}
    VALUES
        {values_str};
    """

    return insert_statement

# input: table_name you're inputting to, data you'd like to insert
# output: array of insert into select statements
# required as snowflake cannot ingest certain values like arrays in values clauses
def generate_insert_into_select_statements(table_name, data):
    insert_into_select_statements = []
    for row in data:
        formatted_values = []
        for col in row:
            formatted_values.append(format_value(col))
        individual_statement = f"INSERT INTO {table_name} SELECT {", ".join(formatted_values)};"
        insert_into_select_statements.append(individual_statement)
    return insert_into_select_statements

def format_columns_for_snowflake(type_mappings):
    pks = []
    columns = []

    for _, col in type_mappings.iterrows():
        col_statement = f"{col['column_name']} {col['column_type']}"
        columns.append(col_statement)
        if col['primary_key'] == "Y":
            pks.append(col['column_name'])
    
    if len(pks) > 0:
        columns.append(f"PRIMARY KEY ({", ".join(pks)})")
    
    return ", ".join(columns)

def get_random_records_sql(table_name, num, cols = []):
    if len(cols) == 0:
        cols.append("$1")
    select_query = f"""
        SELECT {", ".join(cols)} FROM {table_name} 
        ORDER BY RANDOM() 
    """
    return select_query

def generate_delete_query(table_name, ids, id_col_name = "$1"):
    formatted_ids = ", ".join(str(id) for id in ids)
    return f"DELETE FROM {table_name} WHERE {id_col_name} in ({formatted_ids});"

def generate_update_query(table_name, ids, column_to_update, value_to_update_to, id_col_name = "$1"):
    formatted_ids = ", ".join(str(id) for id in ids)
    return f"UPDATE {table_name} SET {column_to_update} = {value_to_update_to} WHERE {id_col_name} IN ({formatted_ids})"