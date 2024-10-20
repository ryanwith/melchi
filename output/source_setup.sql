--This statement uses to a role that should have permissions to perform the following actions.  You may need to use one or more other roles if you do not have access to SECURITYADMIN.
USE ROLE SECURITYADMIN;


--This command creates the change tracking schema.  Not required if it already exists.
CREATE SCHEMA IF NOT EXISTS melchi_cdc_db.streams;


--These statements enable Melchi to create streams that track changes on the provided tables.
ALTER TABLE melchi_db.patients.status SET CHANGE_TRACKING = TRUE;
ALTER TABLE melchi_db.patients.status2 SET CHANGE_TRACKING = TRUE;


--These grants enable Melchi to create objects that track changes.
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE melchi_db_admin;
GRANT USAGE ON DATABASE melchi_cdc_db TO ROLE melchi_db_admin;
GRANT USAGE ON SCHEMA streams TO ROLE melchi_db_admin;
GRANT CREATE TABLE, CREATE STREAM ON SCHEMA melchi_cdc_db.streams TO ROLE melchi_db_admin;


--These grants enable Melchi to read changes from your objects.
GRANT USAGE ON DATABASE melchi_db TO ROLE melchi_db_admin;
GRANT USAGE ON SCHEMA melchi_db.patients TO ROLE melchi_db_admin;
GRANT SELECT ON TABLE melchi_db.patients.status TO ROLE melchi_db_admin;
GRANT SELECT ON TABLE melchi_db.patients.status2 TO ROLE melchi_db_admin;