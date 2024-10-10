import argparse
from src.config import Config
from src.schema_sync import transfer_schema
from src.data_sync import sync_data

def main():
    parser = argparse.ArgumentParser(description="Data Warehouse Transfer Tool")
    parser.add_argument("action", choices=["sync_schema", "sync_data"], help="Action to perform")
    parser.add_argument("--config", required=False, default='config/config.yaml', help="Path to configuration file") 
    
    args = parser.parse_args()

    # Load configuration
    config = Config(config_path=args.config)

    if args.action == "sync_schema":
        print("Schema sync started")
        transfer_schema(config)
    elif args.action == "sync_data":
        print("Data sync started")
        sync_data(config)

if __name__ == "__main__":
    main()