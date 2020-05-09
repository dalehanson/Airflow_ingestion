import airflow.hooks.S3_hook
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import os
import fnmatch
import hashlib
import numpy as np
from random import randint
import gc
import logging
import math
from os import listdir
import snowflake.connector
import re

########################################################################
#Enviroment Setup functions: Defining functions for replicating database structure in Snowflake from Mysql
########################################################################

#Creates Schema in Snowflake
def create_sf_schema(db_name, snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse):
    sf_con = snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse)
    sql = "CREATE SCHEMA " + db_name
    try:
        sf_con.execute(sql)
    except:
        print('Schema already created')
        
        
#Creates ddl (sql statement) for creating 1 table in snowflake
def create_sf_ddl_from_ind_mysql_tbl(db_name, tbl_name, con, exclustion_cols = None):
    tbl_def = pd.read_sql_query('describe ' + db_name + '.' + tbl_name, con) #getting table definitons
    if exclustion_cols is not None:
        tbl_def = tbl_def[~tbl_def.Field.isin(exclustion_cols)] #removing exclustion columns
        
    #Setting the snowflake types based on mysql type
    conditions = [(tbl_def.Type.str.slice(0,3) == 'int') | (tbl_def.Type.str.slice(0,3) == 'tin') | (tbl_def.Type.str.slice(0,3) == 'big') | (tbl_def.Type.str.slice(0,3) == 'med') | (tbl_def.Type.str.slice(0,3) == 'sma'),
                  (tbl_def.Type.str.slice(0,3) == 'var') | (tbl_def.Type.str.slice(0,3) == 'cha') | (tbl_def.Type.str.slice(0,3) == 'tex') | (tbl_def.Type.str.slice(0,3) == 'lon'),
                  (tbl_def.Type.str.slice(0,3) == 'dat') | (tbl_def.Type.str.slice(0,3) == 'tim'),
                  (tbl_def.Type.str.slice(0,3) == 'num') | (tbl_def.Type.str.slice(0,3) == 'dec') | (tbl_def.Type.str.slice(0,3) == 'flo') | (tbl_def.Type.str.slice(0,3) == 'dou'),
                  (tbl_def.Type.str.slice(0,3) == 'boo')]
    choices = ['INT', 'VARCHAR(16777216)','TIMESTAMPNTZ(9)', 'NUMBER(38,20)', 'BOOLEAN']
    
    tbl_def['sf_type'] = np.select(conditions, choices)
    tbl_def['col_state'] = tbl_def['Field'] + ' ' + tbl_def['sf_type'] #produces column statement
    ddl_statement = 'Create  table' + ' ' + db_name + '.' + tbl_name + ' (' + tbl_def['col_state'].str.cat(sep = ' ,' ) + ", ETL_INSERT_TIMESTAMP TIMESTAMP_NTZ(9));"
    return ddl_statement


##Creates all tables from 1 database in mysql enviroment in snowflake using for loop (not in parallel)
def generate_sf_tables_from_ind_mysql_db(db_name,snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, mysql_username, mysql_password, mysql_hostname, mysql_port, exclustion_cols = None ):
    sf_con = snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse)
    mysql_con = mysql_connection(mysql_username, mysql_password, mysql_hostname, mysql_port)
    
    tbls_list = get_table_list(db_name, con = mysql_con)
    if len(tbls_list) == 0:
        print('No Tables in DB')
        mysql_con.close()
        sf_con.close()
        gc.collect() 
    else:
        for tbl_name in tbls_list:
            tbl_ddl = create_sf_ddl_from_ind_mysql_tbl(db_name, tbl_name, mysql_con)
            try:
                sf_con.execute(tbl_ddl)
            except:
                print('Table already exists')
        kill_zombie_connections(mysql_con)
        mysql_con.close()
        sf_con.close()
        gc.collect() 


###Creates all tables for all databases in mysql enviroment in snowflake using for loop (not in parallel)
def generate_sf_multiple_db_tables(db_list, snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse,mysql_username, mysql_password, mysql_hostname, mysql_port):
    for db_name in db_list:
        create_sf_schema(db_name, snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse)
        generate_sf_tables_from_ind_mysql_db(db_name, mysql_username, mysql_password, mysql_hostname, mysql_port)
    gc.collect() 




def load_cdc_date_field_references(database, snowflake_database, sf_con, modified_on_field_name, added_on_field_name):
    
    #Collect all tables for a customer database.  this is used as the base for the date reference
    sql1 = '''SELECT TABLE_SCHEMA, TABLE_NAME FROM {snowflake_database}.INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = upper({database});'''.format(snowflake_database=snowflake_database, database)
        
    tbl_check = pd.read_sql_query(sql1, sf_con)
    #Collecting all tables with columns names, filtered by only pulling in columns named "added_on" or "modified_on".  this table will likely have duplicates for some tables
    sql2 = '''Select * from {snowflake_database}.INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = upper({database})  AND (COLUMN_NAME ILIKE '{modified_on_field_name}%%' OR COLUMN_NAME ILIKE '{added_on_field_name}%%');'''.format(snowflake_database=snowflake_database,database=database,modified_on_field_name=modified_on_field_name, added_on_field_name=added_on_field_name)
            
    date_field_ref = pd.read_sql_query(sql2, sf_con)
    #Collecting tables that have a modified column on it and setting that as the cdc reference
    date_field_ref_to_insert = date_field_ref[date_field_ref['column_name'] == modified_on_field_name][['table_schema','table_name','column_name']]
    #Collecting rest of tables that have an added_on column on it and setting that as the cdc reference (excludes tables that have a "modified_on" column)
    temp_df = pd.merge(date_field_ref[['table_schema','table_name','column_name']], date_field_ref_to_insert, on=['table_name', 'table_schema'], how='left')
    temp_df = temp_df[temp_df['column_name_y'].isnull() == True][['table_schema','table_name','column_name_x']]
    temp_df.columns = ['table_schema','table_name','column_name']
    date_field_ref_to_insert = date_field_ref_to_insert.append(temp_df)
    #Collecting any tables that do not have a modified or added-on column and setting the cdc to pull all data from table
    temp_df = pd.merge(tbl_check, date_field_ref_to_insert, on=['table_name', 'table_schema'], how='left')
    temp_df = temp_df[temp_df['column_name'].isnull() == True]
    temp_df['column_name'] = 'NONEXISTENT'
    date_field_ref_to_insert = date_field_ref_to_insert.append(temp_df)
    #loading data to cdc date column reference table in snowflake
    date_field_ref_to_insert.columns = ['TABLE_SCHEMA', 'TABLE_NAME', 'CDC_DATE_FIELD']
    date_field_ref_to_insert.to_sql('DATE_FIELD_REFERENCE', schema = 'A_UTILITY', con = sf_con, if_exists = 'append', index = False)
    sf_con.close()
    gc.collect()

def load_cdc_date_field_references_multi_dbs(db_list, modified_on_field_name, added_on_field_name, snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse):
    for database in db_list:
        sf_con = snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse)
        load_cdc_date_field_references(database, sf_con, modified_on_field_name, added_on_field_name)
    gc.collect()

########################################################################
#Defining Audit functions
########################################################################

#Gets the last successful datetime that data was pulled over (based on cdc date field reference)
def get_cdc_min_date(source_date_field, db_name, table_name, Task_id, cdc_times_and_dates_folder_local_location, use_unix_format = False):
    AirflowTask = Task_id + db_name.lower() + '_' + table_name.lower()
    file_path = cdc_times_and_dates_folder_local_location + 'previous_cdc_unix.csv'
    if source_date_field == 'NONEXISTENT':
        if use_unix_format == True:
            return 0
        else:
            return '1970-01-01 00:00:00.000'
    df = pd.read_csv(file_path, sep='|')
    df.rename(str.lower, axis='columns', inplace = True)
    df['table_schema'] = df['table_schema'].str.lower()
    df['table_name'] = df['table_name'].str.lower()
    df2 = df[df['airflow_task'] == AirflowTask]
    if use_unix_format == True:
        if df2.shape[0] == 0:
           cdc_min_date = 0 
        else:
            cdc_min_date = df2['cdcmin_unix'].iloc[0]
    else:
        if df2.shape[0] == 0:
            cdc_min_date = '1970-01-01 00:00:00.000'
        else:
            cdc_min_date = datetime.strptime(df2['cdcmin_timestamp'].iloc[0],'%Y-%m-%d %H:%M:%S')
    return cdc_min_date

#gets the max change/modified date for the data by pulling it from the source table
def get_cdc_max_date(source_date_field, source_db_name, source_table_name, source_connection, use_unix_format = False):
    if source_date_field == 'NONEXISTENT':
        if use_unix_format == True:
            return 9223372036
        else:
            return '2262-04-11 23:47:16'
    else:
        table_ref = source_db_name + "." +  source_table_name
        sql_statement = "SELECT IFNULL(MAX(" + source_date_field + "),9223372036) AS CDCMAX FROM " + table_ref
        cdc_max_date = pd.read_sql_query(sql_statement, source_connection)
        if use_unix_format == True:
            cdc_max_date = cdc_max_date.iloc[0,0]
        else:
            cdc_max_date = datetime.utcfromtimestamp(cdc_max_date.iloc[0,0]).strftime('%Y-%m-%d %H:%M:%S')
        return cdc_max_date

#Fetches cdc date filed reference for the table
def get_table_date_field(db_name, table_name, cdc_times_and_dates_folder_local_location):
    file_name_datefield_ref = 'datefield_ref.csv'
    df = pd.read_csv(cdc_times_and_dates_folder_local_location + file_name_datefield_ref, sep = "|")
    df.rename(str.lower, axis='columns', inplace = True)
    df['table_schema'] = df['table_schema'].str.lower()
    df['table_name'] = df['table_name'].str.lower()
    date_field = df[(df['table_schema'] == db_name.lower()) & (df['table_name'] == table_name.lower())]['cdc_date_field'].iloc[0]
    return date_field   


#gets row count for chunk being pulled.  This will be used to populate the audit table
def get_read_rows_from_source(date_field, db_name, table_name, cdc_min, cdc_max, connection ):
    tbl_ref = db_name + '.' + table_name
    if date_field == 'NONEXISTENT':
        sql_statement = 'Select count(*) from ' + tbl_ref
    else:
        sql_statement = 'Select count(*) from ' + tbl_ref + ' where ' + date_field + " > " + str(cdc_min) + " and " + date_field + " <= " + str(cdc_max)
    read_rows = pd.read_sql_query(sql_statement, connection).iloc[0,0]
    return read_rows

def get_sf_query_hist(query_text, sf_connection):
    last_run_statement = "Select MAX(START_TIME) FROM TABLE(information_schema.QUERY_HISTORY_BY_USER('AIRFLOW')) WHERE MD5(QUERY_TEXT) = '" + md5(query_text) + "'"
    last_run_date = str(pd.read_sql_query(last_run_statement, sf_connection).iloc[0,0])
    sql_statement = "SELECT * FROM TABLE(information_schema.QUERY_HISTORY_BY_USER('AIRFLOW')) WHERE MD5(QUERY_TEXT) = '" + md5(query_text) + "' AND START_TIME = '" + last_run_date + "'"
    sf_hist = pd.read_sql_query(sql_statement, sf_connection)
    if sf_hist.shape[0] == 0:
        last_run_statement = "Select IFNULL(MAX(START_TIME),'1970-01-01 00:00:00.000') FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE MD5(QUERY_TEXT) = '" + md5(query_text) + "'"
        last_run_date = str(pd.read_sql_query(last_run_statement, sf_connection).iloc[0,0])
        sql_statement = "SELECT QUERY_ID, IFNULL(ROWS_PRODUCED,0) ROWS_PRODUCED FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE MD5(QUERY_TEXT) = '" + md5(query_text) + "' AND START_TIME = '" + last_run_date + "'"
        sf_hist = pd.read_sql_query(sql_statement, sf_connection)
    return sf_hist

def get_sf_query_hist_v2(snowflake_connection):
    sql_statement = "SELECT *, LAST_QUERY_ID() FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"
    sf_hist = pd.read_sql_query(sql_statement,snowflake_connection)
    return sf_hist
    
#hashes an input string using MD5 method
def md5(string):
    return hashlib.md5(string.encode()).hexdigest()


##Function that starts the audit process for the s3 to snowflake airflow tasks.  Creates a row and loads the row as a csv file which populates most of the data based on the inital data read.  The file will be loaded to the local instance, eventually the files will be loaded to S3 then to the snowflake audit table.  
def start_audit_source_to_s3(Source_System_Name, date_field, db_name, table_name, Task_id, source_connection, cdc_times_and_dates_folder_local_location, migration_audit_folder_path, cdc_min_unix = None, cdc_max_unix = None):
    #########################
    #generate audit info
    #########################
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    end_time = '1900-01-01 00:00:00'
    AirflowTask = Task_id + db_name + '_' + table_name
    SNOW_FLAKE_QUERY_ID = 'Not Applicable'
    Source_System_Name = Source_System_Name
    if cdc_min_unix == None:
        cdc_min_unix = get_cdc_min_date(date_field, db_name, table_name, Task_id, cdc_times_and_dates_folder_local_location, use_unix_format = True)
    if cdc_max_unix == None:
        cdc_max_unix = get_cdc_max_date(date_field, db_name, table_name, source_connection, use_unix_format = True)
    cdc_min_timestamp = str(datetime.fromtimestamp(cdc_min_unix).strftime('%Y-%m-%d %H:%M:%S'))
    cdc_max_timestamp = str(datetime.fromtimestamp(cdc_max_unix).strftime('%Y-%m-%d %H:%M:%S'))
    read_row_count = get_read_rows_from_source(date_field, db_name, table_name, cdc_min_unix, cdc_max_unix, source_connection)
    inserted_row_count = 0
    success = 'N'
    table_schema = db_name.upper()
    table_name = table_name.upper()
    customer_name = db_name.replace('transaction_','').upper()
    df = pd.DataFrame([[AirflowTask, SNOW_FLAKE_QUERY_ID, Source_System_Name,  cdc_min_timestamp, cdc_max_timestamp, cdc_min_unix, cdc_max_unix, start_time, end_time, read_row_count, inserted_row_count, success, table_schema, table_name, customer_name]]
                      , columns=['AIRFLOW_TASK', 'SNOW_FLAKE_QUERY_ID', 'SOURCE_SYSTEM_NAME',  'CDCMIN_TIMESTAMP', 'CDCMAX_TIMESTAMP','CDCMIN_UNIX','CDCMAX_UNIX', 'START_DATETIME', 'END_DATETIME','READ_ROW_COUNT','INSERTED_ROW_COUNT', 'SUCCESS','TABLE_SCHEMA','TABLE_NAME', 'CUSTOMER_NAME'])
    #########################
    #loading audit info to local file system
    #########################
    audit_base_file_name =  'audit_log_' + Task_id + db_name.lower() + '_' + table_name.lower()
    audit_file_list = listdir(migration_audit_folder_path)
    audit_file_list_slim = fnmatch.filter(audit_file_list, audit_base_file_name + "_[0-9][0-9][0-9]" + '*')
    audit_file_list_slim.extend(fnmatch.filter(audit_file_list, audit_base_file_name + "_[0-9]" + '*'))
    if audit_file_list_slim == None:
        file_count = 0
    else:
        file_count = len(audit_file_list_slim)
    file_path = migration_audit_folder_path + audit_base_file_name + '_' + str(file_count) + '.csv'
    df.to_csv(file_path,sep = '|', index = False)
    return df, file_count


##Function that starts the audit process for the s3 to snowflake airflow tasks.  Creates a row and loads the row as a csv file which populates most of the data based on the inital data read.  The file will eventually be loaded to the snowflake audit table.  
def start_audit_snowflake(db_name, table_name, Task_id, Prev_task_id, migration_audit_folder_path):
    #########################
    #loading previous Srouce to S3 audit info from S3 bucket
    #########################
    #prev_task = migration_audit_folder_path + 'audit_log_' + Prev_task_id + db_name.lower() + '_' + table_name.lower()
    
    #hook = airflow.hooks.S3_hook.S3Hook(s3_connection_id)
    #audit_file_list = hook.list_keys(bucket_name = s3_bucket_name, prefix = prev_task)
    
    prev_task = 'audit_log_' + Prev_task_id + db_name.lower() + '_' + table_name.lower()
    audit_file_list = listdir(migration_audit_folder_path)
    audit_file_list_slim = fnmatch.filter(audit_file_list, prev_task + "_[0-9][0-9][0-9]" + '*')
    audit_file_list_slim.extend(fnmatch.filter(audit_file_list, prev_task + "_[0-9]" + '*'))
    prev_audit_df = pd.DataFrame(columns=['AIRFLOW_TASK', 'SNOW_FLAKE_QUERY_ID', 'SOURCE_SYSTEM_NAME',  'CDCMIN_TIMESTAMP', 'CDCMAX_TIMESTAMP','CDCMIN_UNIX','CDCMAX_UNIX', 'START_DATETIME', 'END_DATETIME','READ_ROW_COUNT','INSERTED_ROW_COUNT', 'SUCCESS','TABLE_SCHEMA','TABLE_NAME', 'CUSTOMER_NAME'])
    for i in audit_file_list_slim:
        audit_file = migration_audit_folder_path + i
        audit_row = pd.read_csv(audit_file, sep ="|").replace(np.nan, '', regex=True)
        prev_audit_df = prev_audit_df.append(audit_row, ignore_index = True)
    
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    end_time = '1900-01-01 00:00:00'
    AirflowTask = Task_id + db_name + '_' + table_name
    SNOW_FLAKE_QUERY_ID = ''     
    Source_System_Name = prev_audit_df['SOURCE_SYSTEM_NAME'][0]
    cdc_min_timestamp = min(prev_audit_df['CDCMIN_TIMESTAMP'])
    cdc_max_timestamp = max(prev_audit_df['CDCMAX_TIMESTAMP'])
    cdc_min_unix = min(prev_audit_df['CDCMIN_UNIX'])
    cdc_max_unix = max(prev_audit_df['CDCMAX_UNIX'])
    read_row_count = sum(prev_audit_df[prev_audit_df['SUCCESS'] == 'Y']['READ_ROW_COUNT'])
    inserted_row_count = 0
    success = 'N'
    table_schema = db_name.upper()
    table_name = table_name.upper()
    customer_name = db_name.replace('transaction_','').upper()
    df = pd.DataFrame([[AirflowTask, SNOW_FLAKE_QUERY_ID, Source_System_Name,  cdc_min_timestamp, cdc_max_timestamp, cdc_min_unix, cdc_max_unix, start_time, end_time, read_row_count, inserted_row_count, success, table_schema, table_name, customer_name]]
                      , columns=['AIRFLOW_TASK', 'SNOW_FLAKE_QUERY_ID', 'SOURCE_SYSTEM_NAME',  'CDCMIN_TIMESTAMP', 'CDCMAX_TIMESTAMP','CDCMIN_UNIX','CDCMAX_UNIX', 'START_DATETIME', 'END_DATETIME','READ_ROW_COUNT','INSERTED_ROW_COUNT', 'SUCCESS','TABLE_SCHEMA','TABLE_NAME', 'CUSTOMER_NAME'])
    file_path = migration_audit_folder_path + 'audit_log_' + Task_id + db_name.lower() + '_' + table_name.lower() + '.csv'
    df.to_csv(file_path, sep = '|', index = False)
    #hook.load_string(audit, s3_file_name, s3_bucket_name, replace = True)
    return df


##Function that ends the audit process for both task types.  Updates the audit records in the local instance (updates the endtime, snowflake query id (if applicable), rows successfully migrated, and marks the task as a success).
##Function that ends the audit process for both task types.  Updates the audit records in the local instance (updates the endtime, snowflake query id (if applicable), rows successfully migrated, and marks the task as a success).
def end_audit(audit_df, Task_id, migration_audit_folder_path, sf_connection = None, file_count = "", inserted_rows = 0, sf_sql_statement = None, loop_count=0):
    #########################
    #updating audit info
    #########################
    if sf_connection is not None:
        try:
            hist = pd.read_sql_query("SELECT *, LAST_QUERY_ID() sf_query_id FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-2)))",sf_connection)
            hist.rename(str.lower, axis='columns', inplace = True)
            logging.info(hist.columns)
            sf_query_id = hist['sf_query_id'][0]
            inserted_rows = hist['rows_loaded'][0]
        except:
            logging.info("Query statement NULL not found error occured, setting audit info to default!!!")
            sf_query_id = 'Not Applicable'
            inserted_rows = 0
    else:
        sf_query_id = 'Not Applicable'
        inserted_rows = 0
    
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    audit_df['END_DATETIME'] = end_time
    audit_df['SUCCESS'] = 'Y'
    
    if loop_count == 0:
        audit_df['INSERTED_ROW_COUNT'] = inserted_rows
        audit_df['SNOW_FLAKE_QUERY_ID'] = sf_query_id
    else:
        audit_df['INSERTED_ROW_COUNT'] = inserted_rows + int(audit_df['INSERTED_ROW_COUNT'])
        audit_df['SNOW_FLAKE_QUERY_ID'] = str(sf_query_id) + ', ' + str(audit_df['SNOW_FLAKE_QUERY_ID'])

    #########################
    #loading file to local instance
    #########################
    audit_base_file_name = 'audit_log_' + Task_id + audit_df['TABLE_SCHEMA'][0].lower() + '_' + audit_df['TABLE_NAME'][0].lower()
    if file_count == "":
        file_path = migration_audit_folder_path +  audit_base_file_name + '.csv'
    else:
        file_path = migration_audit_folder_path + audit_base_file_name + '_' + str(file_count) + '.csv'
    audit_df.to_csv(file_path, sep = '|', index = False)





    

########################################################################
#Defining Utility functions
########################################################################


#Create Connection functions
def mysql_connection(mysql_username, mysql_password, mysql_hostname, mysql_port):
    engine = create_engine('mysql://' + mysql_username + ':' + mysql_password + '@' + mysql_hostname + ':' + str(mysql_port) + '/')
    con = engine.connect()
    return con

def snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_stage_schema = ""):
    engine2 = create_engine('snowflake://' + str(snowflake_username) + ":" + str(snowflake_password) +'@' + str(snowflake_account) + '/' + str(snowflake_database) + '/' + str(snowflake_stage_schema) + '?' + 'warehouse=' + str(snowflake_warehouse))
    sf_con = engine2.connect()
    return sf_con

def snowflake_connection_v2(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_stage_schema = ""):
    sf_con = snowflake.connector.connect(
    user=snowflake_username,
    password=snowflake_password,
    account=snowflake_account,
    database = snowflake_database,
    schema = snowflake_stage_schema,
    warehouse = snowflake_warehouse,
    autocommit=False)
    return sf_con

#generates the list of all tables in a mysql database.  This is used to loop through all tables to migrate each table in parallel
def get_table_list(db_name, mysql_username, mysql_password, mysql_hostname, mysql_port, include_database_list = [], exclude_tbls_list = []):
    con = mysql_connection(mysql_username, mysql_password, mysql_hostname, mysql_port)
    sql_statement =  'Show tables from ' + db_name
    tbls = pd.read_sql_query(sql_statement, con)
    tbls.columns = ['table']  
    if len(exclude_tbls_list) > 0:
        tbls = tbls[~tbls.table.isin(exclude_tbls_list)]
    if len(include_database_list ) > 0:
        tbls = tbls[tbls.table.isin(include_database_list)]
    tbls['table'] = tbls['table'].str.strip()
    tbls = tbls['table'].to_list()
    con.close()
    return tbls

#generates the list of all database names in rds instance.  This is used to loop through all database to migrate each table in parallel
def get_database_list(mysql_username, mysql_password, mysql_hostname, mysql_port,trim_by_patterns = None, excluded_databases = None):
    con = mysql_connection(mysql_username, mysql_password, mysql_hostname, mysql_port)
    databases = pd.read_sql_query('show databases', con)
    db_list = databases['Database'].str.strip().to_list()
    if trim_by_patterns is not None:
        db_slim_list = []
        for i in trim_by_patterns:
            db_slim_list.extend(fnmatch.filter(db_list, i))
        db_list = db_slim_list
    if excluded_databases == None:
        excluded_databases = ' '
    excluded_databases = re.sub('[(){}<>\n\r\n]', '', excluded_databases)
    excluded_databases_list = excluded_databases.lower().split(',')
    excluded_databases_list = [x.strip(' ') for x in excluded_databases_list]
    db_list = [x for x in db_list if x not in excluded_databases_list]
    con.close()
    return db_list



#Generates S3 file naming convention
def generate_s3_file_name(db_name, table_name, sep = "_", file_iterator = "", exclude_extention = False, cdc_min_unix = 0, cdc_max_unix = 9223372036, file_extention = 'txt'):
    cdc_min_unix = str(round(cdc_min_unix, 0))
    cdc_max_unix = str(round(cdc_max_unix, 0))
    file_iterator = str(file_iterator)
    if exclude_extention == True:
        s3_file_name = db_name + sep + table_name + sep + cdc_min_unix + sep + cdc_max_unix + '_file_num_' + file_iterator
    else:
        s3_file_name = db_name + sep + table_name + sep + cdc_min_unix + sep + cdc_max_unix + '_file_num_' + file_iterator + '.' + file_extention
        #+ str(cdc_min_date) + '_' + str(cdc_max_date) + '_' + str(file_count)
    return s3_file_name

#kills any mysql connections that were left open but arent doing anything
def kill_zombie_connections(mysql_username = "", mysql_password = "", mysql_hostname = "", mysql_port = "", con = None):
    if con is None:
        con = mysql_connection(mysql_username, mysql_password, mysql_hostname, mysql_port)
    sql = "SHOW FULL PROCESSLIST"
    open_trans = pd.read_sql_query(sql, con)
    dead_trans = open_trans[ open_trans['Time'] >= 90]
    dead_trans = dead_trans.loc[(dead_trans['State'] == '') | (dead_trans['Info'] == 'None')]['Id'].to_list()
    for i in dead_trans:
        sql = "Kill CONNECTION "+ str(i)
        try:
            con.execute(sql)
        except:
            pass
    con.close()

    
def distribute_warehouses():
    rand = randint(1,3)
    snowflake_warehouse = 'MYSQL_TO_RAW_MIGRATION_XSMALL_' + str(rand)
    return snowflake_warehouse
    

#Used in get_chunk_df() function.  Enforces strict limits on chunk size (i.e. ensures no chunks greater than max chunk size).  If there is then the function dynamically adjusts query_date_pull_chunk_size_secs (time divide).  Note that this can  significantly impact program performance.
def chunk_check(skipped_chunks, chunk_size, row_counts, time_divide , date_field, database, table_name, cdc_min, con,  loops = 2):
    if skipped_chunks == 0:
        return row_counts, time_divide
    else:
        logging.info('Need to make time divide smaller, chunk_size too big: time_divide is ' + str(time_divide))
        if time_divide <=1:
            time_divide = 1
        else:
            time_divide = math.floor(time_divide/loops)
        sql = "Select  floor(" + date_field + "/" + str(time_divide) + ") day, count(*) row_count from " + database + '.' + table_name + " WHERE " + date_field + " > " + str(cdc_min) + " group by floor(" + date_field + "/" + str(time_divide) + ") order by floor(" + date_field + "/" + str(time_divide) + ")"
        row_counts = pd.read_sql_query(sql, con)
        row_counts['cumsums'] = row_counts['row_count'].cumsum()
        skipped_chunks = len(row_counts[row_counts['row_count'] > chunk_size])
        if skipped_chunks > 0:
            row_counts, time_divide = chunk_check(skipped_chunks, chunk_size, row_counts, time_divide , date_field, database, table_name, cdc_min, con, loops = loops+1)
        return row_counts, time_divide
    

#creates query chunks based on the cdc date field of the table and creates chunks defined by the "max_rows_per_query_pull" variable.  The "query_date_pull_chunk_size_secs" also affects the behavior of the function by determining how to split up the chunks over a time period that will add up the total rows and get the min and max dates for that total.
def get_chunk_df( database, table_name, date_field, source_connection, strict_chunk_enforcement,  chunk_size = 1000000, time_divide = 86000):
    if date_field == 'NONEXISTENT':
        date_ranges_df  = pd.DataFrame(columns = ['min_date', 'max_date', 'row_count'])
        return date_ranges_df 
    cdc_min = get_cdc_min_date(date_field, database, table_name, 'upload_to_S3_task_', use_unix_format = True)
    sql = "Select  floor(" + date_field + "/" + str(time_divide) + ") day, count(*) row_count from " + database + '.' + table_name + " WHERE " + date_field + " > " + str(cdc_min) + " group by floor(" + date_field + "/" + str(time_divide) + ") order by floor(" + date_field + "/" + str(time_divide) + ")"
    row_counts = pd.read_sql_query(sql, source_connection)
    if row_counts.shape[0] == 0:
        max_date = get_cdc_max_date(date_field, database, table_name, source_connection, use_unix_format = True)
        date_ranges_df = pd.DataFrame({'min_date':cdc_min ,'max_date':max_date,'row_count':0}, index=[0])
        date_ranges_df = date_ranges_df.append(pd.DataFrame({'min_date':max_date ,'max_date': max_date ,'row_count':0}, index=[0]), ignore_index = True)
        return date_ranges_df 
    row_counts['cumsums'] = row_counts['row_count'].cumsum()
    diff = [row_counts['cumsums'][0]]
    diff.extend(row_counts['cumsums'].diff().to_list())
    skipped_chunks = len(list(filter(lambda n: n > chunk_size, diff)))
    if strict_chunk_enforcement.lower in ['true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'certainly', 'uh-huh']: 
        row_counts, time_divide = chunk_check(skipped_chunks, chunk_size, row_counts, time_divide , date_field, database, table_name, cdc_min, source_connection)
    total_rows_left = sum(row_counts['row_count'])
    l = [n for n in range(0, total_rows_left, chunk_size)]
    l.append(total_rows_left)
    date_ranges_df = pd.DataFrame(columns = ['min_date', 'max_date', 'row_count'])
    for z in range(len(l)-1):
        if len(row_counts[(row_counts.cumsums >= l[z]) & (row_counts.cumsums < l[z+1])]) == 0:
            continue
        if z == 0 or date_ranges_df.shape[0] == 0:
            date_ranges_df.loc[len(date_ranges_df)] = [min(row_counts[(row_counts.cumsums >= l[z]) & (row_counts.cumsums <= l[z+1])]['day']) * time_divide, max(row_counts[(row_counts.cumsums >= l[z]) & (row_counts.cumsums <= l[z+1])]['day']) * time_divide, sum(row_counts[(row_counts.cumsums >= l[z]) & (row_counts.cumsums <= l[z+1])]['row_count'])]
            date_ranges_df = date_ranges_df.drop_duplicates()
        else:
            date_ranges_df.loc[len(date_ranges_df)] = [date_ranges_df.loc[len(date_ranges_df)-1,'max_date'], max(row_counts[(row_counts.cumsums >= l[z]) & (row_counts.cumsums <= l[z+1])]['day']) * time_divide,sum(row_counts[(row_counts.cumsums >= l[z]) & (row_counts.cumsums <= l[z+1])]['row_count']) ]
    max_day = max(row_counts['day'])
    max_date = get_cdc_max_date(date_field, database, table_name, source_connection, use_unix_format = True)
    date_ranges_df = date_ranges_df.append(pd.DataFrame({'min_date':max_day * time_divide ,'max_date': max_date ,'row_count':0}, index=[0]), ignore_index = True)
    date_ranges_df = date_ranges_df.append(pd.DataFrame({'min_date':max_date ,'max_date': max_date ,'row_count':0}, index=[0]), ignore_index = True)
    date_ranges_df = date_ranges_df.drop_duplicates()
    date_ranges_df.reset_index()
    if date_ranges_df.shape[0] == 0:
        cdc_max = get_cdc_max_date(date_field, database, table_name, source_connection, use_unix_format = True)
        date_ranges_df.loc[0,'min_date'] = cdc_min
        date_ranges_df.loc[0,'max_date'] = cdc_max
        date_ranges_df.loc[0,'row_count'] = max(row_counts['cumsums'])
        date_ranges_df = date_ranges_df.append(pd.DataFrame({'min_date':cdc_max ,'max_date': cdc_max ,'row_count':0}, index=[0]), ignore_index = True)
        date_ranges_df = date_ranges_df.drop_duplicates()
        date_ranges_df.reset_index()
    gc.collect()
    return date_ranges_df 

#used for chunking files in sets of 1000s, this is to handle the snowflake limitition that has a max of 1000 files for copy into statements
def divide_list_chunks(l, n): 
    for i in range(0, len(l), n):  
        yield l[i:i + n]

########################################################################
#Defining main Airflow Task functions (used in main dag)
########################################################################
    

#The cdc_times_to_local_instance() function takes the last updateded cdc times and date fields from snowflake and loads them to the local instance.  This removes the need for the source to S3 function to not have to query snowflake, saving significant cost (snowflake compute time) and speeding up processing time.
def cdc_times_to_local_instance(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_stage_schema, cdc_times_and_dates_folder_local_location):
    #Connect to Snowflake
    snowflake_warehouse = 'MYSQL_TO_RAW_MIGRATION_XSMALL_1'
    sf_con = snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_stage_schema = snowflake_stage_schema)
    # Build cdc min times query
    query = '''
        SELECT att.airflow_task, att.table_schema, att.table_name, NVL(MAX(att.cdcmax_unix), 0) AS CDCMIN_UNIX, NVL(MAX(att.cdcmax_timestamp), '1970-01-01 00:00:00') AS CDCMIN_TIMESTAMP
        FROM {snowflake_database}.a_utility.audit_task_table att
        INNER JOIN (
            SELECT airflow_task, MAX(end_datetime) AS max_time
            FROM {snowflake_database}.a_utility.audit_task_table
            WHERE success = 'Y'
            GROUP BY airflow_task
        ) mx ON att.airflow_task = mx.airflow_task AND att.end_datetime = mx.max_time
        WHERE att.success = 'Y'
        GROUP BY att.table_schema, att.table_name, att.airflow_task
    '''.format(snowflake_database=snowflake_database)
    # Execute cdc min times query
    cdc_dtf = pd.read_sql_query(query, sf_con)
    # Build date field reference query
    sql_statement = '''SELECT * FROM {snowflake_database}.A_UTILITY.DATE_FIELD_REFERENCE'''.format(snowflake_database=snowflake_database)
    cdc_datefield_df = pd.read_sql_query(sql_statement, sf_con)   
    # Upload to local drive
    file_name_cdc_times = 'previous_cdc_unix.csv'
    cdc_dtf.to_csv(cdc_times_and_dates_folder_local_location + file_name_cdc_times,sep = '|', index = False)
    file_name_datefield_ref = 'datefield_ref.csv'
    cdc_datefield_df.to_csv(cdc_times_and_dates_folder_local_location + file_name_datefield_ref,sep = '|', index = False)
    try:
        sf_con.execute('ALTER WAREHOUSE MYSQL_TO_RAW_MIGRATION_XSMALL_1 SUSPEND')
    except:
        logging.info('Warehouse failed to suspend!')
    sf_con.close()
    gc.collect()

    
    
#Function migrates a table from a mysql database and places it in an s3 bucket.  Connections defined in Airflow Connections respository.
#The mysql_direct_to_file option toggles methods of migrating the data:  
    #  If True, then the SQL server runs the query and loads it to a tmp folder on the container, then loads that file from the temp folder to the S# bucket.  
    #  If False, then Python loads the results of the query into memory, converts it to a string object (csv format) and loads that string object to the S3 bucket (Bypasses saving the file to the container folder but the drawback is that it holds the data in RAM)
def upload_table_to_S3_with_hook(Source_System_Name, bucket_name, database, table_name, Task_id, s3_connection_id, cdc_times_and_dates_folder_local_location, migration_audit_folder_path, mysql_username, mysql_password, mysql_hostname, mysql_port, max_rows_per_text_file= 100000, strict_chunk_enforcement = 'N', chunk_size = 1000000, time_divide = 86000, file_delim = '|', mysql_direct_to_file = False , exclude_columns = False, exclude_cols = []):
    #if table_name == 'cti_ivr_log':
    #    max_rows_per_query_pull = 100000
    logging.info('SETTING AT START!!!')
    hook = airflow.hooks.S3_hook.S3Hook(s3_connection_id)
    con = mysql_connection(mysql_username, mysql_password, mysql_hostname, mysql_port)
    date_field = get_table_date_field(database, table_name, cdc_times_and_dates_folder_local_location)
    logging.info('Date_field set!!!')
    query_chunks_df = get_chunk_df(database, table_name, date_field, source_connection = con, strict_chunk_enforcement=strict_chunk_enforcement, chunk_size = chunk_size, time_divide = time_divide)
    num_chunks = len(query_chunks_df['max_date'])

    #setting table columns
    logging.info('Setting table columns!!!')
    tbl_def = pd.read_sql_query('describe ' + database + '.' + table_name, con)
    if exclude_columns == True:
        tbl_def = tbl_def[~tbl_def.Field.isin(exclude_cols)]
    tbl_def['replace_state'] = np.where((tbl_def['Type'].str.slice(0,3) == 'var') | (tbl_def['Type'].str.slice(0,3) == 'cha') | (tbl_def['Type'].str.slice(0,3) == 'tex') | (tbl_def['Type'].str.slice(0,3) == 'lon'),'replace(replace(replace(replace(A.'+tbl_def['Field']+", '\\n',''),'\\r',''),'\"',''),'|','\"|\"') AS "+ tbl_def['Field'],"A." + tbl_def['Field'])
    logging.info('Table columns handled!!!')
    
    if num_chunks == 0:
        num_chunks = 1
    for chunk_num in range(0, num_chunks):
        logging.info('chunking start: chunk #'+ str(chunk_num))
        if date_field == 'NONEXISTENT':
            audit_df, audit_file_count = start_audit_source_to_s3(Source_System_Name, date_field, database, table_name, Task_id, con, cdc_times_and_dates_folder_local_location, migration_audit_folder_path)
            logging.info('Audit Started!!!')
            sql_statement = "SELECT " + tbl_def['replace_state'].str.cat(sep = ', ' ) + ", now() AS ETL_INSERT_TIMESTAMP FROM " + database + '.' + table_name + " AS A "
            s3_file_name = generate_s3_file_name(database, table_name)
            local_folder_path = 'tmp/' +  s3_file_name
        else:
            audit_df, audit_file_count = start_audit_source_to_s3(Source_System_Name, date_field, database, table_name, Task_id, con, migration_audit_folder_path, cdc_times_and_dates_folder_local_location, cdc_min_unix = query_chunks_df['min_date'][chunk_num], cdc_max_unix = query_chunks_df['max_date'][chunk_num])
            logging.info('Audit Started!!!')
            s3_file_name = generate_s3_file_name(database, table_name, cdc_min_unix =  query_chunks_df['min_date'][chunk_num], cdc_max_unix = query_chunks_df['max_date'][chunk_num])
            local_folder_path = 'tmp/' +  s3_file_name
            sql_statement = "SELECT " + tbl_def['replace_state'].str.cat(sep = ', ' ) + ", now() AS ETL_INSERT_TIMESTAMP FROM " + database + '.' + table_name + " AS A "
            if audit_df['CDCMIN_UNIX'][0] == audit_df['CDCMAX_UNIX'][0]:
                sql_statement = sql_statement + ' where ' +  date_field + ' = ' + str(audit_df['CDCMAX_UNIX'][0])
            else:
                sql_statement = sql_statement + ' where ' +  date_field + ' >= ' + str(audit_df['CDCMIN_UNIX'][0]) + ' and ' + date_field +  ' <' + str(audit_df['CDCMAX_UNIX'][0])
            
            
        
        
        logging.info('SQL Statement Set!!!')
        if mysql_direct_to_file == True:
            sql_statement = sql_statement + " INTO OUTFILE '" + local_folder_path + "' FIELDS TERMINATED BY '" + file_delim + "' LINES TERMINATED BY '\r\n'"  
            con.execute(sql_statement)
            hook.load_file(local_folder_path, s3_file_name, bucket_name)
            os.remove(local_folder_path)
            
        else:
            file_count = 1
            row_count = 0
            for file_chunk in pd.read_sql_query(sql_statement, con, chunksize = max_rows_per_text_file):
                logging.info('Chunking files, file #' + str(file_count) + '!!!')
                mysql_results = file_chunk.to_csv(sep = file_delim, index = False)
                s3_file_name = 'snowflake/' + generate_s3_file_name(database, table_name, file_iterator = file_count, cdc_min_unix = audit_df['CDCMIN_UNIX'][0], cdc_max_unix = audit_df['CDCMAX_UNIX'][0]) #
                hook.load_string(mysql_results, s3_file_name, bucket_name, replace = True)
                row_count += file_chunk.shape[0]
                logging.info('Total rows processed:' + str(row_count) + '!!!')
                file_count += 1
        
        end_audit(audit_df, Task_id, migration_audit_folder_path, file_count = audit_file_count, inserted_rows = row_count, sf_sql_statement = None)
        
    #kill_zombie_connections(con)
    con.close()
    gc.collect()










def upload_table_to_S3_with_hook_v2(Source_System_Name, bucket_name, database, table_name, Task_id, s3_connection_id, cdc_times_and_dates_folder_local_location, migration_audit_folder_path, mysql_username, mysql_password, mysql_hostname, mysql_port, max_rows_per_text_file = 100000, file_delim = '|', mysql_direct_to_file = False , exclude_columns = False, exclude_cols = []):
    #if table_name == 'cti_ivr_log':
    #    max_rows_per_query_pull = 100000
    logging.info('SETTING AT START!!!')
    hook = airflow.hooks.S3_hook.S3Hook(s3_connection_id)
    con = mysql_connection(mysql_username, mysql_password, mysql_hostname, mysql_port)
    date_field = get_table_date_field(database, table_name, cdc_times_and_dates_folder_local_location)
    logging.info('Date_field set!!!')
    audit_df, audit_file_count = start_audit_source_to_s3(Source_System_Name, date_field, database, table_name, Task_id, con, cdc_times_and_dates_folder_local_location, migration_audit_folder_path)
    logging.info('''Audit Started, cdcmin = {cdcmin}, cdcmax = {cdcmax}!!!'''.format(cdcmin = str(audit_df['CDCMIN_UNIX'][0]), cdcmax = str(audit_df['CDCMAX_UNIX'][0])) )
    
    #setting table columns
    logging.info('Setting table columns!!!')
    tbl_def = pd.read_sql_query('describe ' + database + '.' + table_name, con)
    if exclude_columns == True:
        tbl_def = tbl_def[~tbl_def.Field.isin(exclude_cols)]
    tbl_def['replace_state'] = np.where((tbl_def['Type'].str.slice(0,3) == 'var') | (tbl_def['Type'].str.slice(0,3) == 'cha') | (tbl_def['Type'].str.slice(0,3) == 'tex') | (tbl_def['Type'].str.slice(0,3) == 'lon'),'replace(replace(replace(replace(A.'+tbl_def['Field']+", '\\n',''),'\\r',''),'\"',''),'|','\"|\"') AS "+ tbl_def['Field'],"A." + tbl_def['Field'])
    logging.info('Table columns handled!!!')
    
    #setting sql statement
    if date_field == 'NONEXISTENT':
        logging.info('Audit Started!!!')
        sql_statement = "SELECT " + tbl_def['replace_state'].str.cat(sep = ', ' ) + ", now() AS ETL_INSERT_TIMESTAMP FROM " + database + '.' + table_name + " AS A "
        s3_file_name = generate_s3_file_name(database, table_name)
        local_folder_path = 'tmp/' +  s3_file_name
    else:
        s3_file_name = generate_s3_file_name(database, table_name, cdc_min_unix =  audit_df['CDCMIN_UNIX'][0], cdc_max_unix = audit_df['CDCMAX_UNIX'][0])
        local_folder_path = 'tmp/' +  s3_file_name
        sql_statement = "SELECT " + tbl_def['replace_state'].str.cat(sep = ', ' ) + ", now() AS ETL_INSERT_TIMESTAMP FROM " + database + '.' + table_name + " AS A "
        if audit_df['CDCMIN_UNIX'][0] == audit_df['CDCMAX_UNIX'][0]:
            sql_statement = sql_statement + ' where ' +  date_field + ' = ' + str(audit_df['CDCMAX_UNIX'][0])
        else:
            sql_statement = sql_statement + ' where ' +  date_field + ' > ' + str(audit_df['CDCMIN_UNIX'][0])
    logging.info('SQL Statement Set!!!')
            
    #Exporting query results to S3
    if mysql_direct_to_file == True:
        sql_statement = sql_statement + " INTO OUTFILE '" + local_folder_path + "' FIELDS TERMINATED BY '" + file_delim + "' LINES TERMINATED BY '\r\n'"  
        con.execute(sql_statement)
        hook.load_file(local_folder_path, s3_file_name, bucket_name)
        os.remove(local_folder_path)
            
    else:
        file_count = 1
        row_count = 0
        for file_chunk in pd.read_sql_query(sql_statement, con, chunksize = max_rows_per_text_file):
            logging.info('Chunking files, file #' + str(file_count) + '!!!')
            mysql_results = file_chunk.to_csv(sep = file_delim, index = False)
            s3_file_name = 'snowflake/' + generate_s3_file_name(database, table_name, file_iterator = file_count, cdc_min_unix = audit_df['CDCMIN_UNIX'][0], cdc_max_unix = audit_df['CDCMAX_UNIX'][0]) #
            hook.load_string(mysql_results, s3_file_name, bucket_name, replace = True)
            row_count += file_chunk.shape[0]
            logging.info('Total rows processed:' + str(row_count) + '!!!')
            file_count += 1
    
    #end audit and close out connections
    end_audit(audit_df, Task_id, migration_audit_folder_path, file_count = 0, inserted_rows = row_count, sf_sql_statement = None)
    con.close()
    gc.collect()






def upload_to_snowflake(database, table_name, Task_id, Prev_task_id, migration_audit_folder_path, s3_connection_id, s3_bucket_name, snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_stage_schema, sfstage, field_delim = '|'):
    logging.info('SETTING AT START!!!')
    sf_table = database + '.' + table_name
    s3_file_base_name = 'snowflake/' +  database + '_' + table_name
    logging.info('SETTING CHECK BEFORE AUDIT!!!')
    audit_df = start_audit_snowflake(database, table_name, Task_id, Prev_task_id, migration_audit_folder_path)
    
    #getting list of files
    hook = airflow.hooks.S3_hook.S3Hook(s3_connection_id)
    logging.info('SETTING CHECK AFTER HOOK!!!')
    file_list = hook.list_keys(bucket_name = s3_bucket_name, prefix = s3_file_base_name) #hook.list_keys(bucket_name = s3_bucket_name) 
    logging.info('SETTING CHECK AFTER PULLING FILE LIST!!!')
    if file_list == None: 
        logging.info('NOTHING IN FILE_LIST')
        end_audit(audit_df, Task_id, migration_audit_folder_path)
        gc.collect()
        return
    file_list_slim = fnmatch.filter(file_list, s3_file_base_name + "_[0-9][0-9][0-9]" + '*')
    file_list_slim.extend(fnmatch.filter(file_list, s3_file_base_name + "_[0-9]_" + '*'))
    logging.info('SETTING CHECK AFTER FILE_LIST_SLIM!!!')
    if len(file_list_slim) == 0:
        logging.info('FILE_LIST_SLIM = 0!!!')
        end_audit(audit_df, Task_id, migration_audit_folder_path)
        gc.collect()
        return
    else:
        logging.info('FILE_LIST_SLIM > 0!!!')
        file_list_slim_chunks = list(divide_list_chunks(file_list_slim, 1000))
        snowflake_warehouse = distribute_warehouses()
        con =  snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_stage_schema = snowflake_stage_schema)
        if audit_df['CDCMIN_UNIX'][0] == 0 and audit_df['CDCMAX_UNIX'][0] == 9223372036:
            logging.info('NO DATE FIELD FOR TABLE, DELETING TABLE!')
            con.execute('Delete from ' +   sf_table)
        
        loop_count = 0
        for file_list_slim_chunk in file_list_slim_chunks:
            file_list_slim_str = str(file_list_slim_chunk).strip('[]')
        
            #sql copy into statement
            logging.info('SETTING CHECK AFTER BEFORE CREATING SQL STATEMENT!!!')
            copy = ("copy into %s from '@%s'"
                    " files = ( %s )"
                    " file_format = (type = csv field_delimiter = '|'"
                    " skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = None)"
                    % (sf_table, sfstage, file_list_slim_str)
                    )
            if audit_df['CDCMIN_UNIX'][0] == 0 and audit_df['CDCMAX_UNIX'][0] == 9223372036:
                logging.info('NO DATE FIELD FOR TABLE, FORCING LOAD!')
                copy = copy + " Force = True"
            logging.info('SETTING CHECK AFTER BEFORE COPY INTO STATEMENT!!!')
            con.execute(copy)
            logging.info('SETTING CHECK AFTER RUNNING COPY INTO STATEMENT!!!')
            end_audit(audit_df, Task_id, migration_audit_folder_path, sf_connection = con, loop_count = loop_count)
            loop_count = loop_count + 1
        con.close()

        file_list_slim2 = [f.replace('snowflake/', '') for f in file_list_slim]
        for i in file_list_slim2:
            hook.copy_object(source_bucket_key = 'snowflake/' + i, dest_bucket_key =  'snowflake/success/' + i, source_bucket_name = s3_bucket_name,  dest_bucket_name = s3_bucket_name)
        logging.info('SETTING CHECK AFTER MOVING FILES!!!')
        for sub_list in file_list_slim_chunks:
            hook.delete_objects(bucket = s3_bucket_name, keys = sub_list)
    gc.collect()


def load_audit_records_to_s3(migration_audit_folder_path, s3_bucket_name, S3_migration_audit_folder_path):
    command = '''aws s3 cp {migration_audit_folder_path} s3://{s3_bucket_name}/{S3_migration_audit_folder_path} --recursive'''.format(migration_audit_folder_path = migration_audit_folder_path, s3_bucket_name = s3_bucket_name, S3_migration_audit_folder_path = S3_migration_audit_folder_path)
    return command

##Function that loads all audit records on the S3 drive into the snowflake audit table and cleans up the S3 drive
def load_audit_records_to_sf(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_stage_schema, migration_audit_folder_path, s3_connection_id, S3_migration_audit_folder_path, s3_bucket_name, sfstage):
    #########
    #removing audit files on local instance
    #########
    audit_file_list = listdir(migration_audit_folder_path)
    for f in audit_file_list:
        os.remove(migration_audit_folder_path + f)
    #########
    #Loading audit records into snowflake from S3
    #########
    hook = airflow.hooks.S3_hook.S3Hook(s3_connection_id)
    snowflake_warehouse = distribute_warehouses()
    sf_con = snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_stage_schema = snowflake_stage_schema)
    s3_file_list = hook.list_keys(bucket_name = s3_bucket_name, prefix = S3_migration_audit_folder_path)
    try:
        s3_file_list.remove(S3_migration_audit_folder_path)
    except:
        logging.info('audit file list is good')
    file_list2 = list(divide_list_chunks(s3_file_list, 1000))
    for i in file_list2:
        file_list_str = str(i).strip('[]')
        copy = ("copy into %s.A_UTILITY.AUDIT_TASK_TABLE_STAGE"
            " from '@%s'"
            " files = ( %s )"
            " file_format = (type = csv field_delimiter = '|'"
            " skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = None)"
            % (snowflake_database, sfstage, file_list_str)
            )
        sf_con.execute(copy)
        hook.delete_objects(bucket = s3_bucket_name, keys = i)
    copy2 = ("INSERT INTO %s.A_UTILITY.AUDIT_TASK_TABLE (AIRFLOW_TASK,SNOW_FLAKE_QUERY_ID,SOURCE_SYSTEM_NAME,CDCMIN_TIMESTAMP,CDCMAX_TIMESTAMP,CDCMIN_UNIX,CDCMAX_UNIX,START_DATETIME,END_DATETIME,READ_ROW_COUNT,INSERTED_ROW_COUNT,SUCCESS,TABLE_SCHEMA,TABLE_NAME,CUSTOMER_NAME)"
            " SELECT * FROM %s.A_UTILITY.AUDIT_TASK_TABLE_STAGE"
            %(snowflake_database, snowflake_database)
            )
    sf_con.execute(copy2)
    copy3 = ("DELETE FROM %s.A_UTILITY.AUDIT_TASK_TABLE_STAGE"
            %(snowflake_database)
            )
    sf_con.execute(copy3)
    
    sf_con.close()
    gc.collect()




