<!-- readme.md -->

## Overview  📚

<p>Melchi lets you stream live data out of Snowflake and into DuckDB without building and managing custom ETL pipelines.  Simply choose which tables you want to replicate and Melchi will hadnle the rest.</p>

### How it Works

<p>You provide a list of the tables you're looking to stream updates out of.  Melchi then does the following:</p>
<ol>
<li><strong>Creates equivalent tables in your target warehouse.</strong>  It maps your table's schema to your target warehouse determining the column types and creates these tables there.</li>
<li><strong>Creates CDC tables in your source warehouse.</strong>  It creates a stream and permanent table in your source warehouse so that it can track what data has changed and what has already been updated in your target.</li>
<li><strong>Replicates your data in your target warehouse.</strong>  Each time you run this it checks for changes since the last update and moves it into your target warehouse.</li>
</ol>

## Install  📥

```bash
git clone https://github.com/ryanwith/melchi.git
```

## Setup your environment

This project has some dependencies so you should set up a virtual environment to manage them.  One way to do this is with venv.  You can navigate to the melchi folder in the terminal and run the following to set it up.

```bash
python3 -m venv venv
```

You can then activate it.

```bash
source venv/bin/activate
```

Once activated, you should install the dependencies in the requirements.txt file running the following.

```bash
pip install -r requirements.txt
```

## Getting Started

To start enable Melchi to connect to your Snowflake warehouse and move data to DuckDB you need 
to provide some configuration data in config/config.yaml.  For sensitive data you should use env variables.

### Configure your source warehouse parameters

```
source:
  type: snowflake
  account: your_snowflake_account_identifier
  user: your_snowflake_user
  password: your_snowflake_role
  role: melchi_role
  warehouse: your_warehouse
  cdc_strategy: cdc_streams
  cdc_metadata_schema: cdc_db.cdc_schema
  replace_existing: false
```
<strong>Type.</strong>  This is the type of warehouse you will be streaming data from.  For now, it must be set to snowflake.
<strong>Account.</strong>  This is your <a href="https://docs.snowflake.com/en/user-guide/admin-account-identifier">snowflake account identifier</a>.
<strong>User.</strong>  This is username this will use to connect to snowflake.
<strong>Password.</strong>  This is the password you will use to connect to snowflake.  You should store this as an env variable.
<strong>Role.</strong>  This is the role Melchi will use to read changes to your data and create all relevant metadata tables (one stream and one permanent table per table you're replicating).  It must have the following:
<ul>
<li>USAGE granted on all databases it will use</li>
<li>SELECT granted on all tables/views you're replicating.</li>
<li>USAGE granted on all schemas and databases containing these tables/views</li>
<li>CREATE TABLE and CREATE STREAM on the cdc_metadata_schema that Melchi will use.   </li>
<li>USAGE granted on the warehouse you provide.  </li>
<li>Additionally, make sure you GRANT ROLE to the USER you provided above.
</ul>
<strong>Warehouse.</strong>  This is the warehouse Melchi will use for all operations.
<strong>CDC Strategy.</strong>  Must be set to cdc_streams for now.  More strategies may come out in the future.
<strong>CDC metadata schema.</strong>  This is the schema that melchi will create streams and permanent tables needed to capture changes.  It must include the database name as well.  It can be a schema you're streaming data from.  
<strong>Replace existing.</strong>  Recommend setting this to false.  If set to true, Melchi will replace all streams/tables it created in your source warehouse as well as the tables in your target warehouse.  Note--this will not affect the tables you're replicating.

<strong>Note. All tables you want to replicate must have change_tracking set to true.</strong>  You can do this by running the following command for each table.

```
ALTER TABLE table_name SET CHANGE_TRACKING = TRUE;
```

### Configure your target warehouse parameters

```
target:
  type: duckdb
  database: path/to/your/duckdb/database.duckdb
  cdc_metadata_schema: melchi
  replace_existing: true
```
<strong>Type.</strong>  Must be set to duckdb for now.
<strong>Database.</strong>  This is the path to your duckdb database.  You do not have to precreate it--it will be created automatically if it does not exists.
<strong>CDC metadata schema.</strong>  This is the schema that Melchi will create metadata tables in.  It will have two tables: source_columns including how the columns are structured in your source warehouse and captured_tables including metadata about the tables that are replicated.
<strong>Replace existing.</strong>  If set to true, Melchi will replace the replicated tables in your target warehouse.

### Determine the tables you want replicate

```
tables_config:
  path: "config/tables_to_transfer.csv"
```

Create a csv with the columns database, schema, and table.  Store that file in your project, e.g. in config, and set table_config.path in config.yaml to the path to this file.

<strong>Note. All tables you want to replicate must have change_tracking set to true.</strong>  You can do this by running the following command for each table.

```
ALTER TABLE table_name SET CHANGE_TRACKING = TRUE;
```
