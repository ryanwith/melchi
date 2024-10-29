# src/utils/table_config.py

import csv
import os

def get_tables_to_transfer(config):
    tables = []
    csv_path = os.path.join(os.path.dirname(__file__), '..', '..', config.get_tables_config_path())
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            first_field = reader.fieldnames[0]
            
            # Manually check and remove BOM from the first field name if present
            if first_field.startswith('\ufeff'):
                reader.fieldnames[0] = first_field.lstrip('\ufeff')

            i = 1
            for row in reader:
                # Handle both None and empty string cases
                raw_cdc_type = row.get("cdc_type", "FULL_REFRESH")
                database = row['database']
                schema = row['schema']
                table = row['table']
                if any(not val for val in (database, schema, table)):
                    raise ValueError(f"You are missing a database, schema, or table name in row {i}.")
                cdc_type = raw_cdc_type.strip().upper() if raw_cdc_type else "FULL_REFRESH"

                if cdc_type not in ("STANDARD_STREAM", "APPEND_ONLY_STREAM", "FULL_REFRESH"):   
                    raise ValueError(f"{cdc_type} is not a valid CDC type.  Please provide FULL_REFRESH, STANDARD_STREAM, or APPEND_ONLY_STREAM, or leave it blank to default to FULL_REFRESH.")

                tables.append({
                    'database': database,
                    'schema': schema,
                    'table': table,
                    'cdc_type': cdc_type
                })

                i += 1
                
        
                
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found at {csv_path}")
    except csv.Error as e:
        raise csv.Error(f"Error reading CSV file: {e}")
    
    return tables

def get_cdc_type(table_info):
    cdc_type = table_info.get("cdc_type", "FULL_REFRESH").upper()
    if cdc_type not in ("FULL_REFRESH", "STANDARD_STREAM", "APPEND_ONLY_STREAM"):
        raise ValueError(f"{cdc_type} is not a valid CDC type.  Please provide FULL_REFRESH, STANDARD_STREAM, or APPEND_ONLY_STREAM, or leave it blank to default to FULL_REFRESH.")
    return cdc_type