import os
from dotenv import load_dotenv
import os

# Load environment variables from a .env file
load_dotenv()

# Information needed to connect to the source warehouse
SOURCE_WAREHOUSE_CONFIG = {
    "warehouse_type": "snowflake",
    "account_user": os.getenv('ACCOUNT_USER'),
    "account_password": os.getenv('ACCOUNT_PASSWORD'),
    "account_identifier": os.getenv('ACCOUNT_IDENTIFIER'),
}
# Add the tables you want to replicate, the roles you want to use, and the compute you want to use here
# You can use CDC from different databases with different roles and warehouses by using multiple arrays
CDC_TABLE_CONFIG = [
    {
        "database": "melchi_db",
        "role": "melchi_db_admin",
        "warehouse": "compute_wh",
        "cdc_schema": "melchi",
        "tables_to_replicate": ['patients.status']
    }
]

def get_source_warehouse_config():
    return SOURCE_WAREHOUSE_CONFIG

def get_cdc_table_config():
    return CDC_TABLE_CONFIG

# Define more configurations as needed
DEBUG = os.getenv('DEBUG', 'False').lower() in ['true', '1', 't']
