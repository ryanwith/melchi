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

3. This will generate a file named `source_setup.sql` in the `output` directory. Review this file to ensure it meets your security requirements.

4. Execute the SQL statements in the generated file in your Snowflake account to set up the permissions.

Remember to enable change tracking on the tables you want to replicate:

```sql
ALTER TABLE your_db.your_schema.your_table SET CHANGE_TRACKING = TRUE;
```

By following these steps, you'll have the necessary permissions set up in Snowflake for Melchi to operate effectively.

## Usage

Melchi provides several commands to manage your data synchronization process. Here's how to use each of them:

### 1. Generate Source SQL

Before you start using Melchi, you need to set up the necessary permissions and change tracking in Snowflake. Use the following command to generate the required SQL:

```bash
python main.py generate_source_sql --config path/to/your/config.yaml --output path/to/output/directory
```

This will create a `source_setup.sql` file in the specified output directory. Review this file and execute the SQL statements in your Snowflake environment.

### 2. Setup

To initialize Melchi and prepare both your source (Snowflake) and target (DuckDB) for data synchronization, run:

```bash
python main.py setup --config path/to/your/config.yaml
```

This command will:
- Set up change data capture (CDC) in Snowflake for the specified tables
- Create corresponding tables in DuckDB
- Initialize change tracking metadata

### 3. Sync Data

To synchronize data from Snowflake to DuckDB, use:

```bash
python main.py sync_data --config path/to/your/config.yaml
```

This command will:
- Check for changes in the Snowflake tables since the last synchronization
- Apply those changes (inserts, updates, deletes) to the corresponding DuckDB tables

### Best Practices

1. **Initial Setup**: Always run the `setup` command before your first `sync_data` operation.

2. **Regular Syncing**: Set up a scheduled job to run the `sync_data` command at regular intervals to keep your DuckDB instance up-to-date.

3. **Configuration Updates**: If you modify your `config.yaml` or `tables_to_transfer.csv`, re-run the `setup` command to ensure all necessary changes are applied.

4. **Error Handling**: If a `sync_data` operation fails, address the error and re-run the command. Melchi is designed to pick up where it left off.

5. **Monitoring**: Regularly check the Melchi logs to ensure smooth operation and to catch any potential issues early.

### Example Workflow

1. Set up your configuration files (`config.yaml` and `tables_to_transfer.csv`).
2. Generate and execute the source SQL:
   ```bash
   python main.py generate_source_sql --config config/config.yaml --output sql_output
   ```
3. Review and execute the `sql_output/source_setup.sql` in Snowflake.
4. Run the initial setup:
   ```bash
   python main.py setup --config config/config.yaml
   ```
5. Perform your first data sync:
   ```bash
   python main.py sync_data --config config/config.yaml
   ```
6. Set up a cron job or scheduled task to run the sync_data command at your desired frequency.

By following these steps and best practices, you'll be able to efficiently manage your data synchronization process with Melchi.

## How Melchi Works

Melchi uses a combination of Snowflake's change tracking features and custom metadata tables to efficiently synchronize data from Snowflake to DuckDB. Here's a detailed explanation of the process:

### 1. Snowflake Setup

When you run the `setup` command, Melchi performs the following actions in Snowflake:

1. **Enables Change Tracking**: For each table specified in your `tables_to_transfer.csv`, Melchi enables change tracking:
   ```sql
   ALTER TABLE your_db.your_schema.your_table SET CHANGE_TRACKING = TRUE;
   ```

2. **Creates Streams**: Melchi creates a stream for each table to capture changes:
   ```sql
   CREATE STREAM your_db.change_tracking_schema.stream_your_table ON TABLE your_db.your_schema.your_table;
   ```
   The stream name follows the pattern: `change_tracking_schema.stream_[database]$[schema]$[table]`

3. **Creates Processing Tables**: For each stream, Melchi creates a processing table to store captured changes:
   ```sql
   CREATE TABLE your_db.change_tracking_schema.stream_your_table_processing LIKE your_db.your_schema.your_table;
   ```
   Additional metadata columns are added to track the type of change (INSERT, UPDATE, DELETE).

### 2. DuckDB Setup

In DuckDB, Melchi sets up the following:

1. **Replicated Tables**: Creates tables that mirror the structure of the Snowflake tables.

2. **Metadata Table**: Creates a table to track the last synchronization time for each replicated table.

### 3. Data Synchronization Process

When you run the `sync_data` command:

1. **Capture Changes**: Melchi queries the Snowflake streams to capture changes since the last synchronization:
   ```sql
   INSERT INTO stream_your_table_processing 
   SELECT *, METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID 
   FROM stream_your_table;
   ```

2. **Fetch Changes**: The changes are fetched from the processing tables and converted into pandas DataFrames.

3. **Apply Changes**: Melchi applies the changes to the corresponding tables in DuckDB:
   - For INSERTs and UPDATEs, it uses an UPSERT operation (INSERT ... ON CONFLICT DO UPDATE).
   - For DELETEs, it removes the corresponding rows from the DuckDB table.

4. **Update Metadata**: After successful synchronization, Melchi updates the metadata table in DuckDB with the latest synchronization time.

5. **Cleanup**: The processing table in Snowflake is truncated to prepare for the next synchronization cycle.

### Use of DataFrames

Melchi uses pandas DataFrames as an intermediate representation of the data for several reasons:

1. **Efficient Data Manipulation**: DataFrames provide powerful tools for data transformation and filtering.
2. **Type Handling**: They help in managing data type conversions between Snowflake and DuckDB.
3. **Batch Processing**: Large datasets can be efficiently processed in batches using DataFrames.

### Naming Conventions

- Snowflake streams: `change_tracking_schema.stream_[database]$[schema]$[table]`
- Snowflake processing tables: `change_tracking_schema.stream_[database]$[schema]$[table]_processing`
- DuckDB tables: Mirror the original Snowflake table names

Understanding these internals can help you:
- Troubleshoot synchronization issues
- Optimize your Melchi setup for large-scale data transfers
- Extend Melchi's functionality for custom use cases

By leveraging Snowflake's native change tracking and stream features, combined with efficient data processing using pandas, Melchi provides a robust and scalable solution for keeping your DuckDB instance in sync with Snowflake.

### Limitations

Melchi currently does not support the following:

- Geography and Geometry columns.  Those are not supported by standard Snowflake streams.
- Append-only streams.
- Specifying primary keys.  It relies on primary keys set on the table in Snowflake or Snowflake autogenerated row IDs if one are set.
- Modifying tables to transfer without replacing all of them.
- 

## Contributing

We welcome contributions to Melchi! Whether you're fixing bugs, improving documentation, or proposing new features, your efforts are appreciated. Here's how you can contribute:

### Getting Started

1. **Fork the Repository**: Start by forking the Melchi repository to your GitHub account.

2. **Clone the Fork**: Clone your fork to your local machine:
   ```
   git clone https://github.com/your-username/melchi.git
   cd melchi
   ```

3. **Set Up the Development Environment**:
   - Create a virtual environment: `python -m venv venv`
   - Activate it:
     - On Windows: `venv\Scripts\activate`
     - On macOS and Linux: `source venv/bin/activate`
   - Install dependencies: `pip install -r requirements.txt`

### Making Changes

I would love help developing this!  If you're interested please reach out.  I have a discord a

1. **Create a Branch**: Create a new branch for your changes:
   ```
   git checkout -b your-branch-name
   ```

2. **Make Your Changes**: Implement your bug fix or feature.

3. **Follow Coding Standards**:
   - Follow PEP 8 style guide for Python code.
   - Write clear, commented code.
   - Ensure your changes don't break existing functionality.

4. **Write Tests**: If you're adding new functionality, include unit tests. Ensure all tests pass.

5. **Update Documentation**: Update the README or other documentation if necessary.

### Submitting Changes

1. **Commit Your Changes**:
   ```
   git commit -m "Brief description of your changes"
   ```

2. **Push to Your Fork**:
   ```
   git push origin your-branch-name
   ```

3. **Create a Pull Request**: Go to the Melchi repository on GitHub and create a new pull request from your branch.

4. **Describe Your Changes**: In the pull request, provide a clear description of what you've done and why.

5. **Wait for Review**: Maintainers will review your PR. Be open to feedback and be prepared to make additional changes if requested.

### Reporting Issues

- Use the GitHub issue tracker to report bugs or suggest features.
- Before creating a new issue, please check if it already exists.
- Clearly describe the issue, including steps to reproduce for bugs.

### Code of Conduct

- Be respectful and inclusive in your interactions with other contributors.
- We are committed to providing a welcoming and inspiring community for all.

### Questions?

If you have any questions about contributing, feel free to ask in the GitHub issues.

Thank you for contributing to Melchi! Your efforts help make this project better for everyone.

## License

Melchi is released under the Apache License 2.0 with Commons Clause.

This means that while the software is open source, there are certain restrictions on its use:

1. You are free to use, modify, and distribute this software for your own purposes.
2. You can use this software within your organization, whether for commercial purposes or not.
3. You can make modifications to the software and distribute your modified version.

However, the Commons Clause adds the following restriction:

4. You are not permitted to sell the software or any modified version of it as a commercial offering, particularly as a managed service.

In simple terms, you can use Melchi for your own data synchronization needs, but you cannot offer Melchi (or a modified version of it) as a paid service to others.

For the full license text, please see the [LICENSE](LICENSE) file in the repository.

If you have any questions about the license or how you can use Melchi in your specific case, please open an issue in the GitHub repository for clarification.

## Support

If you have any questions you can reach out on <a href="https://discord.gg/MEpEACAz">discord</a>.  
