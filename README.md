# Airflow_ingestion
Set up pipelines in Airflow for ingestion from one database enviroment to another.  The Dag allows you to migrate data for muliple tables and dababases in parallel using change data capture (cdc) methodology.  


Quick description of main code files:
  1. Migration_All_Databases_Mysql_to_Snowflake_functions - Contains all the functions needed to build out the Snowflake Enviroment including source table structure, cdc audit tables, and utility tables
  2.
