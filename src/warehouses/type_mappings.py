# type_mappings.py

import warnings
from ..utils.geometry import convert_geojson_to_wkt

class TypeMapper:
    @staticmethod
    def snowflake_to_duckdb(snowflake_type):
        type_data = snowflake_type.split("(")
        main_type = type_data[0]

        same_names_no_modifiers = ("VARCHAR", "BOOLEAN", "DATE", "TIME")

        if main_type in same_names_no_modifiers:
            return main_type
        elif main_type == "NUMBER":
            return f"DECIMAL({type_data[1]}"
        elif main_type == "FLOAT":
            return "DOUBLE"
        elif main_type == "BINARY":
            return "BLOB"
        elif main_type in ("TIMESTAMP_TZ", "TIMESTAMP_LTZ"):
            return "TIMESTAMPTZ"
        elif main_type == "TIMESTAMP_NTZ":
            precision = int(type_data[1][:-1])
            return f"TIMESTAMP({precision})"
        elif main_type in ("VARIANT", "OBJECT", "ARRAY"):
            return "JSON"
        elif main_type == "VECTOR":
            vector_type = type_data[1].split(",")[0]
            vector_length = type_data[1].split(",")[1][:-1].strip()
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
    
    @staticmethod
    def process_df(source_warehouse, target_warehouse, df):
        try:
            source_warehouse_type = source_warehouse.warehouse_type
            target_warehouse_type = target_warehouse.warehouse_type
            if source_warehouse_type.lower() == "snowflake" and target_warehouse_type.lower() == "duckdb":
                df = process_df_snowflake_to_duckdb(df)
                return df
        except Exception as e:
            print(f"Error processing df: {e}")
            raise

    @staticmethod
    def process_df_snowflake_to_duckdb(df):
        geometry_columns = [col for col in df.columns if 'GEOMETRY' in col.upper() or 'GEOGRAPHY' in col.upper()]
    
        if not geometry_columns:
            return df
        
        df_copy = df.copy()
        for col in geometry_columns:
            df_copy[col] = df_copy[col].apply(convert_geojson_to_wkt)
        
        return df_copy