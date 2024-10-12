import argparse
from src.config import Config
from src.schema_sync import transfer_schema
from src.data_sync import sync_data
from src.source_setup import setup_source

def main():
    parser = argparse.ArgumentParser(description="Data Warehouse Transfer Tool")
    parser.add_argument("action", choices=["setup", "sync_data"], help="Action to perform")
    parser.add_argument("--config", required=False, default='config/config.yaml', help="Path to configuration file") 
    
    args = parser.parse_args()

    # Load configuration
    config = Config(config_path=args.config)

    if args.action == "setup":
        print("Setting up source for CDC")
        setup_source(config)
        print("Source setup complete")
        # print("Setting up target environment")
        transfer_schema(config)
        # print("Target environment setup complete")
    
    elif args.action == "sync_data":
        print("Data sync started")
        sync_data(config)

if __name__ == "__main__":
    main()