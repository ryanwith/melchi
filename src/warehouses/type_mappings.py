import warnings

class TypeMapper:
    @staticmethod
    def snowflake_to_duckdb(snowflake_type):
        type_data = snowflake_type.split("(")
        main_type = type_data[0]

        same_names_no_modifiers = ("FLOAT", "VARCHAR", "BOOLEAN", "DATE", "TIME")

        if main_type in same_names_no_modifiers:
            return main_type
        elif main_type == "NUMBER":
            return f"DECIMAL({type_data[1]}"
        elif main_type == "BINARY":
            return "BLOB"
        elif main_type in ("TIMESTAMP_TZ", "TIMESTAMP_LTZ"):
            return "TIMESTAMPTZ"
        elif main_type == "TIMESTAMP_NTZ":
            precision = int(type_data[1][:-1])
            return f"TIMESTAMP({precision})"
        elif main_type in ("VARIANT", "OBJECT", "JSON", "ARRAY"):
            return "JSON"
        elif main_type == "VECTOR":
            vector_type = type_data[1].split(",")[0]
            vector_length = type_data[1].split(",")[1][:-1]
            return f"{vector_type}[{vector_length}]"
        elif main_type in ("GEOGRAPHY", "GEOMETRY"):
            return "GEOMETRY"
        else:
            warnings.warn(f"Unable to map {snowflake_type} to a duckDB type.  Returning varchar")
            return "VARCHAR"

    @staticmethod
    def duckdb_to_snowflake(duckdb_type):
        mapping = {
            'DECIMAL': 'NUMBER',
            'FLOAT': 'FLOAT',
            'VARCHAR': 'VARCHAR',
            'CHAR': 'CHAR',
            'BOOLEAN': 'BOOLEAN',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'TIMESTAMP': 'TIMESTAMP_NTZ',
            'BLOB': 'BINARY',
            'JSON': 'VARIANT',
        }
        return mapping.get(duckdb_type.upper(), 'VARCHAR')