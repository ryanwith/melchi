class TypeMapper:
    @staticmethod
    def snowflake_to_duckdb(snowflake_type):
        mapping = {
            'NUMBER': 'DECIMAL',
            'FLOAT': 'FLOAT',
            'VARCHAR': 'VARCHAR',
            'CHAR': 'CHAR',
            'STRING': 'VARCHAR',
            'BOOLEAN': 'BOOLEAN',
            'DATE': 'DATE',
            'TIME': 'TIME',
            'TIMESTAMP_NTZ': 'TIMESTAMP',
            'TIMESTAMP_LTZ': 'TIMESTAMP',
            'TIMESTAMP_TZ': 'TIMESTAMP',
            'BINARY': 'BLOB',
            'VARIANT': 'JSON',
            'ARRAY': 'JSON',
            'OBJECT': 'JSON',
        }
        return mapping.get(snowflake_type.upper(), 'VARCHAR')

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