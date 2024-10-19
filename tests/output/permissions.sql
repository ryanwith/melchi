--These grants enable Melchi to create objects that track changes
USE ROLE SECURITYADMIN;
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE melchi_cdc;
GRANT USAGE ON DATABASE cdc_metadata_tables TO ROLE melchi_cdc;
GRANT USAGE ON SCHEMA melchi_metadata TO ROLE melchi_cdc;
GRANT CREATE TABLE, CREATE STREAM ON SCHEMA cdc_metadata_tables.melchi_metadata TO ROLE melchi_cdc;


--These grants enable Melchi to read changes from your objects
GRANT USAGE ON DATABASE melchi_test_data TO ROLE melchi_cdc;
GRANT USAGE ON SCHEMA melchi_test_data.test_melchi_schema TO ROLE melchi_cdc;
GRANT SELECT ON TABLE melchi_test_data.test_melchi_schema.no_pk TO ROLE melchi_cdc;
GRANT SELECT ON TABLE melchi_test_data.test_melchi_schema.one_pk TO ROLE melchi_cdc;
GRANT SELECT ON TABLE melchi_test_data.test_melchi_schema.two_pk TO ROLE melchi_cdc;


--These statements alter tables to allow Melchi to create CDC streams on them
ALTER TABLE melchi_test_data.test_melchi_schema.no_pk SET CHANGE_TRACKING = TRUE;
ALTER TABLE melchi_test_data.test_melchi_schema.one_pk SET CHANGE_TRACKING = TRUE;
ALTER TABLE melchi_test_data.test_melchi_schema.two_pk SET CHANGE_TRACKING = TRUE;