from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator, SubDagOperator
from airflow.executors.local_executor import LocalExecutor
from io import StringIO
from airflow.hooks.base_hook import BaseHook
import airflow.hooks.S3_hook
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from airflow.models import Variable
import pandas as pd
import os
import fnmatch
import hashlib
import numpy as np
from random import randint
import gc
import logging
import math



##################################################################
#Setting variable definitions
##################################################################

parent_dag_name = 'Migration_All_Databases_Mysql_to_Snowflake_v3'

Airflow_snowflake_connection_name = Variable.get('Airflow_snowflake_connection_name')
Airflow_mysql_connection_name = Variable.get('Airflow_mysql_connection_name')
s3_connection_id = Variable.get('s3_connection_name')
s3_bucket_name = Variable.get('s3_main_bucket_name') 
sfstage = Variable.get('snowflake_stage_name') 
strict_chunk_enforcement = Variable.get('strict_chunk_enforcement')
orchestration_country = Variable.get('orchestration_country')

max_rows_per_text_file =  int(Variable.get('max_rows_per_text_file'))
max_rows_per_query_pull =  int(Variable.get('max_rows_per_query_pull'))
query_date_pull_chunk_size_secs = int(Variable.get('query_date_pull_chunk_size_secs'))

Source_System_Name = Variable.get('Source_System_Name')

schedule_interval = Variable.get('Migration_All_Database_Dag_schedule')

database_include_patterns = ['prefix*'] #only inlcude the staging, transaction, and gateway databases, for multiple format as a list seperated by commas
excluded_databases = Variable.get('excluded_databases')

#include_tables = Variable.get('Migration_tables_to_include')
include_tables = 'table1, table2'
include_tables = include_tables.split(',')
include_tables = [x.strip(' ').lower() for x in include_tables]

max_task_time = int(Variable.get('set_task_max_time_minutes')) #set the max runtime for a task
max_task_retries_on_error = int(Variable.get('max_task_retries_on_error'))


migration_audit_folder_path = 'sfolder/' #audit log files for temp storage during run, will be loaded to audit table in snowflake when job is completed




##################################################################
#Collecting Connection attributes from Airflow connections repo
##################################################################


    
sf_con_parm = BaseHook.get_connection(Airflow_snowflake_connection_name) #
snowflake_username = sf_con_parm.login 
snowflake_password = sf_con_parm.password 
snowflake_account = sf_con_parm.host 
snowflake_stage_schema = 'A_UTILITY' 
#snowflake_warehouse = "XSMALL" 
snowflake_database = "sf_db"

mysql_con = BaseHook.get_connection(Airflow_mysql_connection_name)
mysql_username = mysql_con.login 
mysql_password = mysql_con.password 
mysql_hostname = mysql_con.host
mysql_port = mysql_con.port





########################################################################
#Defining Audit functions
########################################################################

#Gets the last successful datetime that data was pulled over (based on cdc date field reference)
def get_cdc_min_date(source_date_field, db_name, table_name, Task_id, connection, use_unix_format = False):
    AirflowTask = Task_id + db_name + '_' + table_name
    if source_date_field == 'NONEXISTENT':
        if use_unix_format == True:
            return 0
        else:
            return '1970-01-01 00:00:00.000'
    if use_unix_format == True:
        sql_statement = ("SELECT IFNULL(MAX(CDCMAX_UNIX),0) AS CDCMIN FROM %s.A_UTILITY.AUDIT_TASK_TABLE where AIRFLOW_TASK = '%s' and success = 'Y'"
                 %(snowflake_database, AirflowTask)
                 )
        cdc_min_date = pd.read_sql_query(sql_statement, connection)
        cdc_min_date = cdc_min_date.iloc[0,0]
    else:
        sql_statement = ("SELECT IFNULL(MAX(CDCMAX_TIMESTAMP),'1900-01-01') AS CDCMIN FROM %s.A_UTILITY.AUDIT_TASK_TABLE where AIRFLOW_TASK = '%s' and success = 'Y'"
                 %(snowflake_database, AirflowTask)
                 )
        cdc_min_date = pd.read_sql_query(sql_statement, connection)
        cdc_min_date = cdc_min_date.iloc[0,0].strftime("%Y-%m-%d %H:%M:%S")
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
        sql_statement = "SELECT IFNULL(MAX(coalesce(UNIX_TIMESTAMP({source_date_field}),{source_date_field})),9223372036) AS CDCMAX FROM {table_ref}".format(source_date_field = source_date_field, table_ref = table_ref)
        cdc_max_date = pd.read_sql_query(sql_statement, source_connection)
        if use_unix_format == True:
            cdc_max_date = cdc_max_date.iloc[0,0]
        else:
            cdc_max_date = datetime.utcfromtimestamp(cdc_max_date.iloc[0,0]).strftime('%Y-%m-%d %H:%M:%S')
        return cdc_max_date

#Fetches cdc date filed reference for the table
def get_table_date_field(db_name, table_name, sf_connection):
    sql_statement = ("SELECT CDC_DATE_FIELD FROM %s.A_UTILITY.DATE_FIELD_REFERENCE "
                     "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'"
                     % (snowflake_database, db_name.upper(), table_name.upper())
                    )
    date_field = pd.read_sql_query(sql_statement.upper(), sf_connection).iloc[0,0]
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



##Function that starts the audit process for the s3 to snowflake airflow tasks.  Creates a row and loads the row as a csv file which populates most of the data based on the inital data read.  The file will eventually be loaded to the snowflake audit table.  
def start_audit_source_to_s3(Source_System_Name, date_field, db_name, table_name, Task_id, source_connection, sf_connection, cdc_min_unix = None, cdc_max_unix = None):
    #########################
    #generate audit info
    #########################
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    end_time = '1900-01-01 00:00:00'
    AirflowTask = Task_id + db_name + '_' + table_name
    SNOW_FLAKE_QUERY_ID = 'Not Applicable'
    Source_System_Name = Source_System_Name
    if cdc_min_unix == None:
        cdc_min_unix = get_cdc_min_date(date_field, db_name, table_name, Task_id, sf_connection, use_unix_format = True)
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
    audit = df.to_csv(sep = '|', index = False)
    #########################
    #loading audit info to S3 bucket
    #########################
    hook = airflow.hooks.S3_hook.S3Hook(s3_connection_id)
    audit_base_file_name = migration_audit_folder_path + 'audit_log_' + Task_id + db_name.lower() + '_' + table_name.lower()
    audit_file_list = hook.list_keys(bucket_name = s3_bucket_name, prefix = audit_base_file_name)
    if audit_file_list == None:
        file_count = 0
    else:
        file_count = len(audit_file_list)
    s3_file_name = audit_base_file_name + '_' + str(file_count) + '.csv'
    hook.load_string(audit, s3_file_name, s3_bucket_name, replace = True)
    return df, file_count


##Function that starts the audit process for the s3 to snowflake airflow tasks.  Creates a row and loads the row as a csv file which populates most of the data based on the inital data read.  The file will eventually be loaded to the snowflake audit table.  
def start_audit_snowflake(db_name, table_name, Task_id, Prev_task_id):
    #########################
    #loading previous Srouce to S3 audit info from S3 bucket
    #########################
    prev_task = migration_audit_folder_path + 'audit_log_' + Prev_task_id + db_name.lower() + '_' + table_name.lower()
    hook = airflow.hooks.S3_hook.S3Hook(s3_connection_id)
    audit_file_list = hook.list_keys(bucket_name = s3_bucket_name, prefix = prev_task)
    prev_audit_df = pd.DataFrame(columns=['AIRFLOW_TASK', 'SNOW_FLAKE_QUERY_ID', 'SOURCE_SYSTEM_NAME',  'CDCMIN_TIMESTAMP', 'CDCMAX_TIMESTAMP','CDCMIN_UNIX','CDCMAX_UNIX', 'START_DATETIME', 'END_DATETIME','READ_ROW_COUNT','INSERTED_ROW_COUNT', 'SUCCESS','TABLE_SCHEMA','TABLE_NAME', 'CUSTOMER_NAME'])
    for i in audit_file_list:
        audit_file = hook.select_key(i, bucket_name = s3_bucket_name)
        audit_row = pd.read_csv(StringIO(audit_file), sep ="|").replace(np.nan, '', regex=True)
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
    audit = df.to_csv(sep = '|', index = False)
    s3_file_name = migration_audit_folder_path + 'audit_log_' + Task_id + db_name.lower() + '_' + table_name.lower() + '.csv'
    hook.load_string(audit, s3_file_name, s3_bucket_name, replace = True)
    return df


##Function that ends the audit process for both task types.  Updates the audit records in S3 (updates the endtime, snowflake query id (if applicable), rows successfully migrated, and marks the task as a success).
def end_audit(audit_df, Task_id, sf_connection = None, file_count = "", inserted_rows = 0, sf_sql_statement = None):
    #########################
    #updating audit info
    #########################
    if sf_connection is not None:
        hist = pd.read_sql_query("SELECT *, LAST_QUERY_ID() sf_query_id FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-2)))",sf_connection)
        if hist.shape[0] > 0:
            logging.info(hist.columns)
            sf_query_id = hist['sf_query_id'][0]
            inserted_rows = hist['rows_loaded'][0]
        else:
            sf_query_id = 'Not Applicable'
            inserted_rows = 0
    else:
        sf_query_id = 'Not Applicable'
        inserted_rows = 0
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    audit_df['END_DATETIME'] = end_time
    audit_df['SNOW_FLAKE_QUERY_ID'] = sf_query_id
    audit_df['INSERTED_ROW_COUNT'] = inserted_rows
    audit_df['SUCCESS'] = 'Y'
    #########################
    #loading file back to S3
    #########################
    audit = audit_df.to_csv(sep = '|', index = False)
    audit_base_file_name = migration_audit_folder_path + 'audit_log_' + Task_id + audit_df['TABLE_SCHEMA'][0].lower() + '_' + audit_df['TABLE_NAME'][0].lower()
    if file_count == "":
        s3_file_name = audit_base_file_name + '.csv'
    else:
        s3_file_name = audit_base_file_name + '_' + str(file_count) + '.csv'
    hook = airflow.hooks.S3_hook.S3Hook(s3_connection_id)
    hook.load_string(audit, s3_file_name, s3_bucket_name, replace = True)



    

########################################################################
#Defining Utility functions
########################################################################


#Create Connection functions
def mysql_connection(username, password, hostname, port):
    engine = create_engine('mysql://' + username + ':' + password + '@' + hostname + ':' + str(port) + '/')
    con = engine.connect()
    return con

def snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_stage_schema = ""):
    engine2 = create_engine('snowflake://' + str(snowflake_username) + ":" + str(snowflake_password) +'@' + str(snowflake_account) + '/' + str(snowflake_database) + '/' + str(snowflake_stage_schema) + '?' + 'warehouse=' + str(snowflake_warehouse))
    sf_con = engine2.connect()
    return sf_con


#generates the list of all tables in a mysql database.  This is used to loop through all tables to migrate each table in parallel
def get_table_list(db_name, include_database_list = [], exclude_tbls_list = []):
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
def get_database_list(trim_by_patterns = None, excluded_databases = None):
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
def kill_zombie_connections(con = None):
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
    rand = randint(1,25)
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
def get_chunk_df( database, table_name, date_field, source_connection, snowflake_connection, chunk_size = 1000000, time_divide = 86000):
    if date_field == 'NONEXISTENT':
        date_ranges_df  = pd.DataFrame(columns = ['min_date', 'max_date', 'row_count'])
        return date_ranges_df 
    cdc_min = get_cdc_min_date(date_field, database, table_name, 'upload_to_S3_task_', snowflake_connection, use_unix_format = True)
    #sql = "Select  floor(" + date_field + "/" + str(time_divide) + ") day, count(*) row_count from " + database + '.' + table_name + " WHERE " + date_field + " > " + str(cdc_min) + " group by floor(" + date_field + "/" + str(time_divide) + ") order by floor(" + date_field + "/" + str(time_divide) + ")"
    
    sql = "Select  floor(coalesce( UNIX_TIMESTAMP({date_field}), {date_field})/{time_divide}) day, count(*) row_count from {database}.{table_name} WHERE coalesce( UNIX_TIMESTAMP({date_field}), {date_field}) > {cdc_min} group by floor(coalesce( UNIX_TIMESTAMP({date_field}), {date_field})/{time_divide}) order by floor(coalesce( UNIX_TIMESTAMP({date_field}), {date_field})/{time_divide})".format(date_field = date_field, time_divide = str(time_divide), database = database, table_name = table_name, cdc_min = str(cdc_min))
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
    


    
    
    
    
#Function migrates a table from a mysql database and places it in an s3 bucket.  Connections defined in Airflow Connections respository.
#The mysql_direct_to_file option toggles methods of migrating the data:  
    #  If True, then the SQL server runs the query and loads it to a tmp folder on the container, then loads that file from the temp folder to the S# bucket.  
    #  If False, then Python loads the results of the query into memory, converts it to a string object (csv format) and loads that string object to the S3 bucket (Bypasses saving the file to the container folder but the drawback is that it holds the data in RAM)
def upload_table_to_S3_with_hook(Source_System_Name, bucket_name, database, table_name, Task_id, file_delim = '|', mysql_direct_to_file = False , exclude_columns = False, exclude_cols = []):
    #if table_name == 'cti_ivr_log':
    #    max_rows_per_query_pull = 100000
    logging.info('SETTING AT START!!!')
    hook = airflow.hooks.S3_hook.S3Hook(s3_connection_id)
    snowflake_warehouse = distribute_warehouses()
    con = mysql_connection(mysql_username, mysql_password, mysql_hostname, mysql_port)
    sf_con = snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_stage_schema)
    date_field = get_table_date_field(database, table_name, sf_con)
    logging.info('Date_field set!!!')
    query_chunks_df = get_chunk_df(database, table_name, date_field, source_connection = con, snowflake_connection = sf_con, chunk_size = max_rows_per_query_pull, time_divide = query_date_pull_chunk_size_secs)
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
            audit_df, audit_file_count = start_audit_source_to_s3(Source_System_Name, date_field, database, table_name, Task_id, con, sf_con)
            logging.info('Audit Started!!!')
            sql_statement = "SELECT " + tbl_def['replace_state'].str.cat(sep = ', ' ) + ", now() AS ETL_INSERT_TIMESTAMP FROM " + database + '.' + table_name + " AS A "
            s3_file_name = generate_s3_file_name(database, table_name)
            local_folder_path = 'tmp/' +  s3_file_name
        else:
            audit_df, audit_file_count = start_audit_source_to_s3(Source_System_Name, date_field, database, table_name, Task_id, con, sf_con, cdc_min_unix = query_chunks_df['min_date'][chunk_num], cdc_max_unix = query_chunks_df['max_date'][chunk_num])
            logging.info('Audit Started!!!')
            s3_file_name = generate_s3_file_name(database, table_name, cdc_min_unix =  query_chunks_df['min_date'][chunk_num], cdc_max_unix = query_chunks_df['max_date'][chunk_num])
            local_folder_path = 'tmp/' +  s3_file_name
            cols = tbl_def['replace_state'].str.cat(sep = ', ' )
            sql_statement = "SELECT {cols}, now() AS ETL_INSERT_TIMESTAMP FROM {database}.{table_name} AS A ".format(cols = cols, database = database, table_name = table_name)
            if audit_df['CDCMIN_UNIX'][0] == audit_df['CDCMAX_UNIX'][0]:
                sql_statement = '{sql_statement} where coalesce(UNIX_TIMESTAMP({date_field}),{date_field}) = {cdc_max}'.format(sql_statement = sql_statement, date_field = date_field, cdc_max = str(audit_df['CDCMAX_UNIX'][0]))
            else:
                sql_statement = '{sql_statement} where coalesce(UNIX_TIMESTAMP({date_field}),{date_field}) >= {cdc_min} and coalesce(UNIX_TIMESTAMP({date_field}),{date_field}) < {cdc_min} '.format(sql_statement = sql_statement, date_field = date_field, cdc_min = str(audit_df['CDCMIN_UNIX'][0]), cdcmax = str(audit_df['CDCMAX_UNIX'][0]))
                #sql_statement = sql_statement + ' where ' +  date_field + ' >= ' + str(audit_df['CDCMIN_UNIX'][0]) + ' and ' + date_field +  ' <' + str(audit_df['CDCMAX_UNIX'][0])
            
            
        
        
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
        
        end_audit(audit_df, Task_id, file_count = audit_file_count, inserted_rows = row_count, sf_sql_statement = None)
        
    #kill_zombie_connections(con)
    con.close()
    sf_con.close()
    gc.collect()



def upload_to_snowflake(database, table_name, Task_id, Prev_task_id, field_delim = '|'):
    logging.info('SETTING AT START!!!')
    sf_table = database + '.' + table_name
    s3_file_base_name = 'snowflake/' +  database + '_' + table_name
    snowflake_warehouse = distribute_warehouses()
    con =  snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_stage_schema)
    logging.info('SETTING CHECK BEFORE AUDIT!!!')
    audit_df = start_audit_snowflake(database, table_name, Task_id, Prev_task_id)
    
    #getting list of files
    hook = airflow.hooks.S3_hook.S3Hook(s3_connection_id)
    logging.info('SETTING CHECK AFTER HOOK!!!')
    file_list = hook.list_keys(bucket_name = s3_bucket_name, prefix = s3_file_base_name) #hook.list_keys(bucket_name = s3_bucket_name) 
    logging.info('SETTING CHECK AFTER PULLING FILE LIST!!!')
    if file_list == None: 
        logging.info('NOTHING IN FILE_LIST')
        end_audit(audit_df, Task_id)
        con.close()
        con.close()
        gc.collect()
        return
    file_list_slim = fnmatch.filter(file_list, s3_file_base_name + "_[0-9][0-9][0-9]" + '*')
    file_list_slim.extend(fnmatch.filter(file_list, s3_file_base_name + "_[0-9]_" + '*'))
    logging.info('SETTING CHECK AFTER FILE_LIST_SLIM!!!')
    if len(file_list_slim) == 0:
        logging.info('FILE_LIST_SLIM = 0!!!')
        end_audit(audit_df, Task_id)
        con.close()
        con.close()
        gc.collect()
        return
    else:
        logging.info('FILE_LIST_SLIM > 0!!!')
        file_list_slim = fnmatch.filter(file_list, s3_file_base_name + "_[0-9][0-9][0-9]" + '*')
        file_list_slim.extend(fnmatch.filter(file_list, s3_file_base_name + "_[0-9]_" + '*'))
        file_list_slim_chunks = list(divide_list_chunks(file_list_slim, 1000))
        if audit_df['CDCMIN_UNIX'][0] == 0 and audit_df['CDCMAX_UNIX'][0] == 9223372036:
            logging.info('NO DATE FIELD FOR TABLE, DELETING TABLE!')
            con.execute('Delete from ' +   sf_table)
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
            end_audit(audit_df, Task_id, sf_connection = con)

        file_list_slim2 = [f.replace('snowflake/', '') for f in file_list_slim]
        for i in file_list_slim2:
            hook.copy_object(source_bucket_key = 'snowflake/' + i, dest_bucket_key =  'snowflake/success/' + i, source_bucket_name = s3_bucket_name,  dest_bucket_name = s3_bucket_name)
        logging.info('SETTING CHECK AFTER MOVING FILES!!!')
        for sub_list in file_list_slim_chunks:
            hook.delete_objects(bucket = s3_bucket_name, keys = sub_list)
        
        
    con.close()
    gc.collect()



##Function that loads all audit records on the S3 drive into the snowflake audit table and cleans up the S3 drive
def load_audit_records():
    snowflake_warehouse = distribute_warehouses()
    sf_con = snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_stage_schema = snowflake_stage_schema)
    hook = airflow.hooks.S3_hook.S3Hook(s3_connection_id)
    file_list = hook.list_keys(bucket_name = s3_bucket_name, prefix = migration_audit_folder_path)
    try:
        file_list.remove(migration_audit_folder_path)
    except:
        logging.info('audit file list is good')
    file_list2 = list(divide_list_chunks(file_list, 1000))
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



#############################################################################
#Defining SubDag structure
#############################################################################
    
default_args = {
    'owner': 'dale'
    ,'start_date': datetime(2020, 3, 9)
    ,'retry_delay': timedelta(minutes=.25)
    ,'retries': max_task_retries_on_error
}

def database_sub_dag(parent_dag_name, database_name, schedule_interval):
    one_dag =  DAG(parent_dag_name + '.' + database_name, default_args=default_args, schedule_interval=schedule_interval, concurrency = 50, catchup = False) 
        
    # Creates the tasks dynamically.  Each one will elaborate one chunk of data.
    def create_dynamic_task_tos3(table):
        return PythonOperator(
            #provide_context=True,
            task_id='upload_to_S3_task_' + database_name + '_' + table,
            python_callable=upload_table_to_S3_with_hook,
            op_kwargs={		   
            'Source_System_Name': Source_System_Name,
            'Task_id': 'upload_to_S3_task_',
            'bucket_name': s3_bucket_name,
            'table_name': table,
            'database': database_name
            #'exclude_columns': False
            },
            dag=one_dag)
            
    def create_dynamic_task_tosf(table):
        return PythonOperator(
            #provide_context=True,
            task_id='upload_to_snowflake_task_' + database_name + '_' + table,
            python_callable=upload_to_snowflake,
            op_kwargs={
            'table_name': table,
            'Task_id': 'upload_to_snowflake_task_',
            'Prev_task_id': 'upload_to_S3_task_',
            'database': database_name,
            },
            trigger_rule="all_done",
            dag=one_dag)
    
    
    tbl_list = get_table_list(database_name, include_tables) #collecting all table names from database database
    
    for t in tbl_list:
        dt_s3 = create_dynamic_task_tos3(t)
        dt_sf = create_dynamic_task_tosf(t)
        dt_s3 >> dt_sf

    return one_dag




#############################################################################
#Defining Main Dag structure
#############################################################################

main_dag = DAG(
    dag_id=parent_dag_name
    ,default_args=default_args
    ,schedule_interval=schedule_interval #00 23 * * *
    #schedule_interval=timedelta(minutes=5),
    #max_active_runs=1
    ,concurrency = 3
    ,catchup = False
)

start_task = DummyOperator(
        task_id='start_task',
        dag=main_dag
)
    
 
kill_zombie_cons = PythonOperator(
        task_id ='kill_zombie_connections',
        python_callable=kill_zombie_connections,
        trigger_rule="all_done",
        dag=main_dag
 )   
      
clean_up_audit_records = PythonOperator(
    task_id='clean_up_audit_records',
    python_callable=load_audit_records,
    trigger_rule="all_done",
    dag=main_dag
)


    
    #end dummy dag  
end = DummyOperator(
    task_id='end',
    dag=main_dag
)



database_list =  get_database_list(trim_by_patterns = database_include_patterns, excluded_databases = excluded_databases) 


#Each database is an independant task that will run in parallel4
def subdag_task(database):
    sub_dag = SubDagOperator(
        subdag = database_sub_dag(parent_dag_name, database, schedule_interval),
        task_id= database,
        dag=main_dag,
        pool='Pool_max_parallel_500',
        executor=LocalExecutor()
    )
    return sub_dag


for i in database_list:
    sbt = subdag_task(i)
    start_task >> sbt
    sbt >> kill_zombie_cons
    kill_zombie_cons >> clean_up_audit_records
    clean_up_audit_records >> end
