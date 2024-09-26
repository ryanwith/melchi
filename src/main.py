import argparse
import time
from util.database import create_connection, setup_cdc_tables, run_cdc

def main():
    parser = argparse.ArgumentParser(description="Manage Snowflake data operations.")
    parser.add_argument('--setup', help="Create CDC tables according to what is specified in the configuration file.", action="store_true")
    parser.add_argument('--run', type=int, help="Run the application for a specified number of seconds")

    args = parser.parse_args()  # Parse the arguments from the command line


    if args.setup:
        try:
            conn = create_connection()
            cursor = conn.cursor()
            setup_cdc_tables(cursor)

        finally:
            if cursor:
                cursor.close()  # Close the cursor
            if conn:
                conn.close()  # Close the connection

    elif args.run is not None:
        print(f"Running the application for {args.run} seconds...")
        interval = args.run
        conn = create_connection()
        cursor = conn.cursor()

        try:
            while True:
                run_cdc(cursor)
                time.sleep(interval)  # Sleep for some time before the next ingestion

        except KeyboardInterrupt:
            print("CDC capture stopped.")

        finally:
            if cursor:
                cursor.close()  # Close the cursor
            if conn:
                conn.close()  # Close the connection
    
    else:
        print("No valid args provided")
        # Your running code here, using args.run for the duration in seconds
if __name__ == "__main__":
    main()