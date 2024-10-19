# main.py

import argparse
from src.config import Config
from src.schema_sync import transfer_schema
from src.data_sync import sync_data
from src.source_setup import setup_source
from src.generate_permissions import generate_snowflake_source_permissions, write_permissions_to_file  # Updated import

def main():
    parser = argparse.ArgumentParser(description="Data Warehouse Transfer Tool")
    parser.add_argument("action", choices=["setup", "sync_data", "generate_permissions"], help="Action to perform")
    parser.add_argument("--config", required=False, default='config/config.yaml', help="Path to configuration file") 
    
    args = parser.parse_args()

    # Load configuration
    config = Config(config_path=args.config)

    if args.action == "setup":
        print("Setting up source for CDC")
        setup_source(config)
        print("Source setup complete")
        transfer_schema(config)
    elif args.action == "sync_data":
        print("Data sync started")
        sync_data(config)
    elif args.action == "generate_permissions":
        if config.source_type == 'snowflake':
            write_permissions_to_file(generate_snowflake_source_permissions(config))
        else:
            print(f"Permission generation for {config.source_type} is not yet supported.")

if __name__ == "__main__":
    main()