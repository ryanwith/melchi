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
                raw_cdc_type = row.get("cdc_type")
                database = row['database']
                schema = row['schema']
                table = row['table']
                if any(not val for val in (database, schema, table)):
                    raise ValueError(f"You are missing a database, schema, or table name in row {i}.")
                cdc_type = raw_cdc_type.strip().upper() if raw_cdc_type else "STANDARD"

                if cdc_type not in ("STANDARD", "APPEND_ONLY"):   
                    raise ValueError(f"{cdc_type} is not a valid CDC type.  Please provide STANDARD, APPEND_ONLY, or leave it blank to default to STANDARD.")

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