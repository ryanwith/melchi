# src/generate_permissions.py

from src.utils.table_config import get_tables_to_transfer

def generate_snowflake_source_permissions(config):
    role = config.source_config['role']
    warehouse = config.source_config['warehouse']
    change_tracking_database = config.source_config['change_tracking_database']
    change_tracking_schema = config.source_config['change_tracking_schema']
    tables = get_tables_to_transfer(config)

    general_grants = [
        f"USE ROLE SECURITYADMIN;",
        f"GRANT USAGE ON WAREHOUSE {warehouse} TO ROLE {role};",
        f"GRANT USAGE ON DATABASE {change_tracking_database} TO ROLE {role};",
        f"GRANT USAGE ON SCHEMA {change_tracking_schema} TO ROLE {role};",
        f"GRANT CREATE TABLE, CREATE STREAM ON SCHEMA {change_tracking_database}.{change_tracking_schema} TO ROLE {role};",
    ]

    database_grants = []
    schema_grants = []
    table_grants = []
    alter_table_statements = []


    print(general_grants)
    for table in tables:
        database = table['database']
        schema = table['schema']
        table_name = table['table']
        database_grants.append(f"GRANT USAGE ON DATABASE {database} TO ROLE {role};")
        schema_grants.append(f"GRANT USAGE ON SCHEMA {database}.{schema} TO ROLE {role};")
        table_grants.append(f"GRANT SELECT ON TABLE {database}.{schema}.{table_name} TO ROLE {role};")
        alter_table_statements.append(f"ALTER TABLE {database}.{schema}.{table_name} SET CHANGE_TRACKING = TRUE;")

    database_grants = sorted(list(set(database_grants)))
    schema_grants = sorted(list(set(schema_grants)))


    general_grants.insert(0, "--These grants enable Melchi to create objects that track changes")
    database_grants.insert(0, "--These grants enable Melchi to read changes from your objects")
    alter_table_statements.insert(0, "--These statements alter tables to allow Melchi to create CDC streams on them")

    general_grants.append("\n")
    table_grants.append("\n")

    all_grants = general_grants + database_grants + schema_grants + table_grants + alter_table_statements

    return "\n".join(all_grants)

def write_permissions_to_file(permissions, filename = "output/permissions.sql"):
    with open(filename, 'w') as f:
        f.write(permissions)
    print(f"Permissions written to {filename}")