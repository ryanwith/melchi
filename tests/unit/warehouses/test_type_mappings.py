# tests/unit/test_type_mappings.py

import unittest
import warnings
from src.warehouses.type_mappings import TypeMapper

class TestTypeMapper(unittest.TestCase):

    def test_snowflake_to_duckdb_simple_types(self):
        simple_types = ["VARCHAR", "BOOLEAN", "DATE", "TIME"]
        for type_name in simple_types:
            with self.subTest(type_name=type_name):
                self.assertEqual(TypeMapper.snowflake_to_duckdb(type_name), type_name)

    def test_snowflake_to_duckdb_number(self):
        self.assertEqual(TypeMapper.snowflake_to_duckdb("NUMBER(10,2)"), "DECIMAL(10,2)")

    def test_snowflake_to_duckdb_float(self):
        self.assertEqual(TypeMapper.snowflake_to_duckdb("FLOAT"), "DOUBLE")

    def test_snowflake_to_duckdb_binary(self):
        self.assertEqual(TypeMapper.snowflake_to_duckdb("BINARY"), "BLOB")

    def test_snowflake_to_duckdb_timestamps(self):
        self.assertEqual(TypeMapper.snowflake_to_duckdb("TIMESTAMP_TZ"), "TIMESTAMPTZ")
        self.assertEqual(TypeMapper.snowflake_to_duckdb("TIMESTAMP_LTZ"), "TIMESTAMPTZ")
        self.assertEqual(TypeMapper.snowflake_to_duckdb("TIMESTAMP_NTZ(9)"), "TIMESTAMP(9)")

    def test_snowflake_to_duckdb_semi_structured(self):
        semi_structured_types = ["VARIANT", "OBJECT", "ARRAY"]
        for type_name in semi_structured_types:
            with self.subTest(type_name=type_name):
                self.assertEqual(TypeMapper.snowflake_to_duckdb(type_name), "JSON")

    def test_snowflake_to_duckdb_vector(self):
        self.assertEqual(TypeMapper.snowflake_to_duckdb("VECTOR(FLOAT, 16)"), "FLOAT[16]")

    def test_snowflake_to_duckdb_geography_geometry(self):
        self.assertEqual(TypeMapper.snowflake_to_duckdb("GEOGRAPHY"), "GEOMETRY")
        self.assertEqual(TypeMapper.snowflake_to_duckdb("GEOMETRY"), "GEOMETRY")

    def test_snowflake_to_duckdb_unsupported_type(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = TypeMapper.snowflake_to_duckdb("UNSUPPORTED_TYPE")
            self.assertEqual(result, "VARCHAR")
            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[-1].category, UserWarning))
            self.assertIn("Unable to map UNSUPPORTED_TYPE to a duckDB type", str(w[-1].message))

    def test_duckdb_to_snowflake_simple_mappings(self):
        mappings = {
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
        for duckdb_type, snowflake_type in mappings.items():
            with self.subTest(duckdb_type=duckdb_type):
                self.assertEqual(TypeMapper.duckdb_to_snowflake(duckdb_type), snowflake_type)

    def test_duckdb_to_snowflake_case_insensitive(self):
        self.assertEqual(TypeMapper.duckdb_to_snowflake('decimal'), 'NUMBER')
        self.assertEqual(TypeMapper.duckdb_to_snowflake('VARCHAR'), 'VARCHAR')

    def test_duckdb_to_snowflake_unsupported_type(self):
        self.assertEqual(TypeMapper.duckdb_to_snowflake('UNSUPPORTED_TYPE'), 'VARCHAR')

if __name__ == '__main__':
    unittest.main()