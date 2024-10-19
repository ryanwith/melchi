<!-- readme.md -->
# Melchi

## Overview  📚

Melchi is a data synchronization tool that streamlines the process of replicating data from Snowflake to DuckDB in near real-time. It eliminates the need to build and manage custom ETL pipelines, saving time and resources for data teams.

Here's how Melchi works:

1. You provide a list of Snowflake tables you want to replicate.
2. Run the setup command in the terminal.
3. Melchi automatically:
   - Creates equivalent tables in DuckDB based on your Snowflake table schemas
   - Sets up streams and change tracking tables in Snowflake for each replicated table
   - Creates change tracking tables in DuckDB to monitor update times

Once set up, simply run the `sync_data` command whenever you need to update. Melchi efficiently checks Snowflake for inserts, updates, and deletes, and applies these changes to DuckDB.

All you need to do is set up a role in Snowflake with the appropriate permissions. Melchi handles the rest, providing a low-maintenance, efficient solution for keeping your DuckDB instance in sync with Snowflake.

## Installation  📥

### Prerequisites

- Python 3.7 or later
- Git

### Steps

1. Clone the repository:
   ```bash
   git clone https://github.com/ryanwith/melchi.git
   cd melchi
   ```

2. Set up a virtual environment:
   ```bash
   python3 -m venv venv
   ```

3. Activate the virtual environment:
   - On macOS and Linux:
     ```bash
     source venv/bin/activate
     ```
   - On Windows:
     ```bash
     venv\Scripts\activate
     ```

4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

5. Set up environment variables:
   Create a `.env` file in the project root directory and add your Snowflake and DuckDB credentials:
   ```
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   DUCKDB_DATABASE_PATH=/path/to/your/duckdb/database.db
   ```

6. Verify the installation:
   ```bash
   python main.py --help
   ```
   If you see the help message with available commands, Melchi is installed correctly.

### Troubleshooting

If you encounter any issues during installation, please check the following:
- Ensure you're using Python 3.7 or later
- Make sure all environment variables are set correctly
- Check that you have the necessary permissions to install packages and create directories

For more detailed troubleshooting, please refer to our [documentation](link_to_docs) or open an issue on our GitHub repository.

## Usage

[Add usage instructions here]

## Configuration

Melchi uses a YAML configuration file to manage connections and specify which tables to replicate. Follow these steps to set up your configuration:

1. Create a `config.yaml` file in the project root directory.

2. Add the following sections to your `config.yaml`:

   ```yaml
   source:
     type: snowflake
     account: ${SNOWFLAKE_ACCOUNT_IDENTIFIER}
     user: ${SNOWFLAKE_USER}
     password: ${SNOWFLAKE_PASSWORD}
     role: snowflake_role_to_use
     warehouse: snowflake_warehouse_to_use
     change_tracking_database: database_with_change_tracking_schema
     change_tracking_schema: name_of_change_tracking_schema
     cdc_strategy: cdc_streams

   target:
     type: duckdb
     database: /path/to/your/local/duckdb/database.duckdb
     change_tracking_schema: name_of_change_tracking_schema

   tables_config:
     path: "config/tables_to_transfer.csv"
   ```

   Replace placeholders with your actual Snowflake and DuckDB details.

3. Create a `tables_to_transfer.csv` file in the `config` directory to specify which tables to replicate:

   ```csv
   database,schema,table
   your_db,your_schema,table1
   your_db,your_schema,table2
   ```

4. Set up environment variables in a `.env` file:

   ```
   SNOWFLAKE_ACCOUNT_IDENTIFIER=your_account
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   DUCKDB_DATABASE_PATH=/path/to/your/duckdb/database.db
   ```

Ensure all configuration files are properly set up before running Melchi.

## Permissions

To use Melchi effectively, you need to set up the correct permissions in Snowflake. Here's how to do it:

1. Create a dedicated role in Snowflake for Melchi:

   ```sql
   USE ROLE SECURITYADMIN;
   CREATE ROLE melchi_role;
   ```

2. Grant the necessary permissions to this role. You can do this manually or use Melchi's `generate_source_sql` feature to help you.

### Manual Permission Setup

If you prefer to set up permissions manually, you need to grant the following:

- Usage on the warehouse
- Usage on the databases and schemas containing the tables you want to replicate
- Select permission on the tables you want to replicate
- Create Table and Create Stream permissions on the change tracking schema

For example:

```sql
GRANT USAGE ON WAREHOUSE your_warehouse TO ROLE melchi_role;
GRANT USAGE ON DATABASE your_db TO ROLE melchi_role;
GRANT USAGE ON SCHEMA your_db.your_schema TO ROLE melchi_role;
GRANT SELECT ON TABLE your_db.your_schema.your_table TO ROLE melchi_role;
GRANT CREATE TABLE, CREATE STREAM ON SCHEMA change_tracking_db.change_tracking_schema TO ROLE melchi_role;
```

### Using generate_permissions

Melchi provides a `generate_permissions` feature to help you create the necessary SQL statements for setting up permissions. To use it:

1. Ensure your `config.yaml` and `tables_to_transfer.csv` are correctly set up.

2. Run the following command:

   ```bash
   python main.py generate_permissions
   ```

3. This will generate a file named `permissions.sql` in the `output` directory. Review this file to ensure it meets your security requirements.

4. Execute the SQL statements in the generated file in your Snowflake account to set up the permissions.

Remember to enable change tracking on the tables you want to replicate:

```sql
ALTER TABLE your_db.your_schema.your_table SET CHANGE_TRACKING = TRUE;
```

By following these steps, you'll have the necessary permissions set up in Snowflake for Melchi to operate effectively.

## Usage

[Add usage information here]

## Contributing

[Add contributing guidelines here]

## License

[Add license information here]

## Support

[Add support information here]
