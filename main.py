# main.py

import argparse
from src.config import Config
from src.schema_sync import transfer_schema
from src.data_sync import sync_data
from src.source_setup import setup_source
from src.source_sql_generator import generate_source_sql

def main():
    parser = argparse.ArgumentParser(description="Data Warehouse Transfer Tool")
    parser.add_argument("action", choices=["setup", "sync_data", "generate_source_sql"], help="Action to perform")
    parser.add_argument("--config", required=False, default='config/config.yaml', help="Path to configuration file")
    parser.add_argument("--output", required=False, default='output', help="Output directory for generated SQL")
    parser.add_argument("--replace-existing", action="store_true", help="Recreates all CDC objects in the source warehouse, all metadata objects in the target warehouse, and recreates all tables in the target warehouse")
    args = parser.parse_args()

    # Load configuration
    config = Config(config_path=args.config)
    config.source_config["replace_existing"] = args.replace_existing
    config.target_config["replace_existing"] = args.replace_existing

    if args.action == "setup":
        print("Setting up source for CDC")
        setup_source(config)
        print("Source setup complete")
        transfer_schema(config)
    elif args.action == "sync_data":
        print("Data sync started")
        sync_data(config)
    elif args.action == "generate_source_sql":
        file_location = "output" if args.output == None else args.output
        generate_source_sql(config, file_location)
        print(f"{config.source_type.capitalize()} change tracking setup SQL written to {file_location}/source_setup.sql.  Please review the generated SQL and execute it in your {config.source_type} environment.")
        print(f"IMPORTANT: These SQL files need to be executed by a {config.source_type} user with appropriate permissions.")


if __name__ == "__main__":
    main()