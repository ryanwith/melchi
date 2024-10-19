# # main.py

# import argparse
# from src.config import Config
# from src.schema_sync import transfer_schema
# from src.data_sync import sync_data
# from src.source_setup import setup_source
# from src.generate_permissions import generate_snowflake_source_permissions, write_permissions_to_file  # Updated import

# def main():
#     parser = argparse.ArgumentParser(description="Data Warehouse Transfer Tool")
#     parser.add_argument("action", choices=["setup", "sync_data", "generate_permissions"], help="Action to perform")
#     parser.add_argument("--config", required=False, default='config/config.yaml', help="Path to configuration file") 
    
#     args = parser.parse_args()

#     # Load configuration
#     config = Config(config_path=args.config)

#     if args.action == "setup":
#         print("Setting up source for CDC")
#         setup_source(config)
#         print("Source setup complete")
#         transfer_schema(config)
#     elif args.action == "sync_data":
#         print("Data sync started")
#         sync_data(config)
#     elif args.action == "generate_permissions":
#         if config.source_type == 'snowflake':
#             write_permissions_to_file(generate_snowflake_source_permissions(config))
#         else:
#             print(f"Permission generation for {config.source_type} is not yet supported.")

# if __name__ == "__main__":
#     main()

# main.py

import argparse
from src.config import Config
from src.schema_sync import transfer_schema
from src.data_sync import sync_data
from src.source_setup import setup_source
from src.source_sql_generator import generate_source_permissions, generate_source_change_tracking_setup, write_to_file

def main():
    parser = argparse.ArgumentParser(description="Data Warehouse Transfer Tool")
    parser.add_argument("action", choices=["setup", "sync_data", "generate_source_sql"], help="Action to perform")
    parser.add_argument("--config", required=False, default='config/config.yaml', help="Path to configuration file")
    parser.add_argument("--output", required=False, default='output', help="Output directory for generated SQL")
    
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
    elif args.action == "generate_source_sql":
        permissions_sql = generate_source_permissions(config)
        write_to_file(permissions_sql, f"{args.output}/{config.source_type}_permissions.sql")
        print(f"{config.source_type.capitalize()} permissions SQL written to {args.output}/{config.source_type}_permissions.sql")
        
        change_tracking_sql = generate_source_change_tracking_setup(config)
        write_to_file(change_tracking_sql, f"{args.output}/{config.source_type}_change_tracking.sql")
        print(f"{config.source_type.capitalize()} change tracking setup SQL written to {args.output}/{config.source_type}_change_tracking.sql")
        
        print(f"\nIMPORTANT: These SQL files need to be executed by a {config.source_type} user with appropriate permissions.")
        print(f"Please review the generated SQL and execute it in your {config.source_type} environment.")

if __name__ == "__main__":
    main()