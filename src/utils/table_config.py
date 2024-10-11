import csv
import os

def get_tables_to_transfer(config_path='config/tables_to_transfer.csv'):
    tables = []
    csv_path = os.path.join(os.path.dirname(__file__), '..', '..', config_path)
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            first_field = reader.fieldnames[0]
            
            # Manually check and remove BOM from the first field name if present
            if first_field.startswith('\ufeff'):
                reader.fieldnames[0] = first_field.lstrip('\ufeff')

            for row in reader:
                tables.append({
                    'database': row['database'],
                    'schema': row['schema'],
                    'table': row['table']
                })
                
    except FileNotFoundError:
        print(f"Warning: Configuration file not found at {csv_path}")
        return []
    except csv.Error as e:
        print(f"Error reading CSV file: {e}")
        return []
    
    return tables