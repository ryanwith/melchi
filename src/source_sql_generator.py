# src/source_sql_generator.py

from src.utils.table_config import get_tables_to_transfer

def generate_snowflake_source_sql(config):
    role = config.source_config['role']
    warehouse = config.source_config['warehouse']
    change_tracking_database = config.source_config['change_tracking_database']
    change_tracking_schema = config.source_config['change_tracking_schema']
    tables = get_tables_to_transfer(config)

    use_role_statement = [
        "--This statement uses to a role that should have permissions to perform the following actions.  You may need to use one or more other roles if you do not have access to SECURITYADMIN.",
        "USE ROLE SECURITYADMIN;",
        "\n"
        ]

    create_change_tracking_schema_statement = [
        "--This command creates the change tracking schema.  Not required if it already exists.",
        f"CREATE SCHEMA IF NOT EXISTS {change_tracking_database}.{change_tracking_schema};",
        "\n"
    ]

    general_grants = [
        "--These grants enable Melchi to create objects that track changes.",
        f"GRANT USAGE ON WAREHOUSE {warehouse} TO ROLE {role};",
        f"GRANT USAGE ON DATABASE {change_tracking_database} TO ROLE {role};",
        f"GRANT USAGE ON SCHEMA {change_tracking_schema} TO ROLE {role};",
        f"GRANT CREATE TABLE, CREATE STREAM ON SCHEMA {change_tracking_database}.{change_tracking_schema} TO ROLE {role};",
    ]

    enable_cdc_statements = ["--These statements enable Melchi to create streams that track changes on the provided tables."]
    database_grants = []
    schema_grants = []
    table_grants = []

    for table in tables:
        database = table['database']
        schema = table['schema']
        table_name = table['table']
        database_grants.append(f"GRANT USAGE ON DATABASE {database} TO ROLE {role};")
        schema_grants.append(f"GRANT USAGE ON SCHEMA {database}.{schema} TO ROLE {role};")
        table_grants.append(f"GRANT SELECT ON TABLE {database}.{schema}.{table_name} TO ROLE {role};")
        enable_cdc_statements.append(f"ALTER TABLE {database}.{schema}.{table_name} SET CHANGE_TRACKING = TRUE;")

    database_grants = sorted(list(set(database_grants)))
    schema_grants = sorted(list(set(schema_grants)))

    database_grants.insert(0, "--These grants enable Melchi to read changes from your objects.")

    general_grants.append("\n")

    all_grants = use_role_statement + create_change_tracking_schema_statement + enable_cdc_statements + ["\n"] + general_grants + database_grants + schema_grants + table_grants

    return "\n".join(all_grants)

def write_permissions_to_file(permissions, filename = "output/permissions.sql"):
    with open(filename, 'w') as f:
        f.write(permissions)
    print(f"Permissions written to {filename}")