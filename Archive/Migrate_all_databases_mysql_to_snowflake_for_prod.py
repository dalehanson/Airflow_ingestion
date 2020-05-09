
from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator, SubDagOperator
from airflow.executors.local_executor import LocalExecutor
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



##################################################################
#Setting variable definitions
##################################################################


#mysql_database = Variable.get('Create_customer_database_tables_var__database_name')
Airflow_snowflake_connection_name = Variable.get('Airflow_snowflake_connection_name')
Airflow_mysql_connection_name = Variable.get('Airflow_mysql_connection_name')
s3_connection_id = Variable.get('s3_connection_name')
s3_bucket_name = Variable.get('s3_main_bucket_name') 
sfstage = Variable.get('snowflake_stage_name') 

max_rows_per_text_file = int(Variable.get('max_rows_per_text_file'))
max_rows_per_query_pull = int(Variable.get('max_rows_per_query_pull'))
query_date_pull_chunk_size_secs = int(Variable.get('query_date_pull_chunk_size_secs'))

Source_System_Name = Variable.get('Source_System_Name')

parent_dag_name = 'Migrage_all_databases_mysql_to_snowflake'

database_include_patterns = ['prefix*'] #only inlcude the staging, transaction, and gateway databases, for multiple format as a list seperated by commas

excluded_tables = ['table1','table2']  #list of tables we dont want to migrate

max_task_time = int(Variable.get('set_task_max_time_minutes')) #set the max runtime for a task
max_task_retries_on_error = int(Variable.get('max_task_retries_on_error'))



##################################################################
#Collection Connection attributes from Airflow connections repo
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
        sql_statement = ("SELECT IFNULL(MAX(CDCMAX_UNIX),0) AS CDCMIN FROM (SELECT CDCMAX_UNIX FROM US_RAW.A_UTILITY.AUDIT_TASK_TABLE where AIRFLOW_TASK = '" + AirflowTask + "' and success = 'Y'"
                 " UNION"
                 " Select CDCMAX_UNIX from US_RAW.A_UTILITY.AUDIT_TASK_TABLE_OVERFLOW where AIRFLOW_TASK = '" + AirflowTask + "'  and success = 'Y') sub"
                 )
        cdc_min_date = pd.read_sql_query(sql_statement, connection)
        cdc_min_date = cdc_min_date.iloc[0,0]
    else:
        sql_statement = ("SELECT IFNULL(MAX(CDCMAX_TIMESTAMP),'1900-01-01') AS CDCMIN FROM (SELECT CDCMAX_TIMESTAMP FROM US_RAW.A_UTILITY.AUDIT_TASK_TABLE where AIRFLOW_TASK = '" + AirflowTask + "' and success = 'Y'"
                 " UNION"
                 " Select CDCMAX_TIMESTAMP from US_RAW.A_UTILITY.AUDIT_TASK_TABLE_OVERFLOW where AIRFLOW_TASK = '" + AirflowTask + "'  and success = 'Y') sub"
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
        sql_statement = "SELECT IFNULL(MAX(" + source_date_field + "),9223372036) AS CDCMAX FROM " + table_ref
        cdc_max_date = pd.read_sql_query(sql_statement, source_connection)
        if use_unix_format == True:
            cdc_max_date = cdc_max_date.iloc[0,0]
        else:
            cdc_max_date = datetime.utcfromtimestamp(cdc_max_date.iloc[0,0]).strftime('%Y-%m-%d %H:%M:%S')
        return cdc_max_date

#Fetches cdc date filed reference for the table
def get_table_date_field(db_name, table_name, sf_connection):
    sql_statement = ("SELECT CDC_DATE_FIELD FROM US_RAW.A_UTILITY.DATE_FIELD_REFERENCE "
                     "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'"
                     % (db_name, table_name)
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
    
#hashes an input string using MD5 method
def md5(string):
    return hashlib.md5(string.encode()).hexdigest()


def distribute_audit_inserts():
    rand = randint(1,2)
    if rand == 1:
        audit_table = 'AUDIT_TASK_TABLE'
    else:
        audit_table = 'AUDIT_TASK_TABLE_OVERFLOW'
    return audit_table


#Function that starts the audit process.  Creates a row in the snowflake audit table and populates most of the data based on the inital data read.  this is specifically for the source to s3 airflow tasks
def start_audit_source_to_s3(Source_System_Name, date_field, db_name, table_name, Task_id, source_connection, sf_connection
                             , cdc_min_unix = None, cdc_max_unix = None, audit_table = 'AUDIT_TASK_TABLE'):
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    end_time = '1900-01-01 00:00:00'
    AirflowTask = Task_id + db_name + '_' + table_name
    SNOW_FLAKE_QUERY_ID = ''
    Source_System_Name = Source_System_Name
    if cdc_min_unix == None:
        cdc_min_unix = get_cdc_min_date(date_field, db_name, table_name, Task_id, sf_connection, use_unix_format = True)
    if cdc_max_unix == None:
        #cdc_max_timestamp = get_cdc_max_date(date_field, db_name, table_name, source_connection)
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
                      , columns=['AIRFLOW_TASK', 'SNOW_FLAKE_QUERY_ID', 'SOURCE_SYSTEM_NAME',  'CDCMIN_TIMESTAMP', 'CDCMAX_TIMESTAMP','CDCMIN_UNIX','CDCMAX_UNIX', 'START_DATETIME', 'END_DATETIME','read_row_count','inserted_row_count', 'success','TABLE_SCHEMA','TABLE_NAME', 'CUSTOMER_NAME'])
    df.to_sql(audit_table, schema = 'A_UTILITY', con = sf_connection, if_exists = 'append', index = False)
    return df

##Function that starts the audit process.  Creates a row in the snowflake audit table and populates most of the data based on the inital data read.  this is specifically for the s3 to snowflake airflow tasks
def start_audit_snowflake(db_name, table_name, Task_id, Prev_task_id, sf_connection, audit_table = 'AUDIT_TASK_TABLE'):
    prev_task = Prev_task_id + db_name + '_' + table_name
    max_prev_task_key = pd.read_sql_query("Select max(PROCESS_TASK_KEY) FROM US_RAW.A_UTILITY.AUDIT_TASK_TABLE WHERE AIRFLOW_TASK = '" + prev_task + "'", sf_connection).iloc[0,0]
    prev_audit_df = pd.read_sql_query("SELECT * FROM US_RAW.A_UTILITY.AUDIT_TASK_TABLE WHERE PROCESS_TASK_KEY = " + str(max_prev_task_key), sf_connection)
    
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    end_time = '1900-01-01 00:00:00'
    AirflowTask = Task_id + db_name + '_' + table_name
    SNOW_FLAKE_QUERY_ID = ''     
    Source_System_Name = prev_audit_df['source_system_name'][0]
    cdc_min_timestamp = prev_audit_df['cdcmin_timestamp'][0]
    cdc_max_timestamp = prev_audit_df['cdcmax_timestamp'][0]
    cdc_min_unix = prev_audit_df['cdcmin_unix'][0]
    cdc_max_unix = prev_audit_df['cdcmax_unix'][0]
    read_row_count = prev_audit_df['inserted_row_count'][0]
    inserted_row_count = 0
    success = 'N'
    table_schema = db_name.upper()
    table_name = table_name.upper()
    customer_name = db_name.replace('transaction_','').upper()
    df = pd.DataFrame([[AirflowTask, SNOW_FLAKE_QUERY_ID, Source_System_Name,  cdc_min_timestamp, cdc_max_timestamp, cdc_min_unix, cdc_max_unix, start_time, end_time, read_row_count, inserted_row_count, success, table_schema, table_name, customer_name]]
                      , columns=['AIRFLOW_TASK', 'SNOW_FLAKE_QUERY_ID', 'SOURCE_SYSTEM_NAME',  'CDCMIN_TIMESTAMP', 'CDCMAX_TIMESTAMP','CDCMIN_UNIX','CDCMAX_UNIX', 'START_DATETIME', 'END_DATETIME','read_row_count','inserted_row_count', 'success','TABLE_SCHEMA','TABLE_NAME', 'CUSTOMER_NAME'])
    df.to_sql(audit_table, schema = 'A_UTILITY', con = sf_connection, if_exists = 'append', index = False)
    return df


##Function that ends the audit process.  Updates the audit row in the snowflake audit table that was created in the start audit function run.  this updates the endtime, snowflake query id (if applicable), rows successfully migrated, and marks the task as a success.  This is used for both aiflow tasks
def end_audit(db_name, table_name, sf_connection, Task_id, inserted_rows = 0, sf_sql_statement = None, audit_table = 'AUDIT_TASK_TABLE'):
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    AirflowTask = Task_id + db_name + '_' + table_name
    max_task_key = pd.read_sql_query("Select max(PROCESS_TASK_KEY) FROM US_RAW.A_UTILITY." + audit_table + " WHERE AIRFLOW_TASK = '" + AirflowTask + "'", sf_connection).iloc[0,0]
    if sf_sql_statement == None:
        sf_query_id = 'Not Applicable'
    else:
        hist = get_sf_query_hist(sf_sql_statement, sf_connection)
        if hist.shape[0] > 0:
            sf_query_id = hist['query_id'][0]
            inserted_rows = hist['rows_produced'][0]
        else:
            sf_query_id = 'Not Applicable'
            inserted_rows = 0
    update = ("UPDATE US_RAW.A_UTILITY.%s SET "
          "END_DATETIME = '%s'"
          ", SNOW_FLAKE_QUERY_ID = '%s'"
          ", INSERTED_ROW_COUNT = %s"
          ", SUCCESS = 'Y'"
          " WHERE PROCESS_TASK_KEY = %s" 
          ";"
          %(audit_table,end_time, sf_query_id, inserted_rows, max_task_key)
    )
    pd.read_sql_query(update, sf_connection)






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
def get_table_list(db_name, exclude_tables = False, exclude_tbls_list = []):
    con = mysql_connection(mysql_username, mysql_password, mysql_hostname, mysql_port)
    sql_statement =  'Show tables from ' + db_name
    tbls = pd.read_sql_query(sql_statement, con)
    tbls.columns = ['table']  
    if exclude_tables == True:
        tbls = tbls[~tbls.table.isin(exclude_tbls_list)]
    tbls['table'] = tbls['table'].str.strip()
    tbls = tbls['table'].to_list()
    con.close()
    return tbls

#generates the list of all database names in rds instance.  This is used to loop through all database to migrate each table in parallel
def get_database_list(trim_by_patterns = None):
    con = mysql_connection(mysql_username, mysql_password, mysql_hostname, mysql_port)
    databases = pd.read_sql_query('show databases', con)
    db_list = databases['Database'].str.strip().to_list()
    if trim_by_patterns is not None:
        db_slim_list = []
        for i in trim_by_patterns:
            db_slim_list.extend(fnmatch.filter(db_list, i))
        db_list = db_slim_list
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
def kill_zombie_connections(con):
    sql = "SHOW FULL PROCESSLIST"
    open_trans = pd.read_sql_query(sql, con)
    dead_trans = open_trans[ open_trans['Time'] >= 90]
    dead_trans = dead_trans.loc[dead_trans['State'] == '']['Id'].to_list()
    for i in dead_trans:
        sql = "Kill CONNECTION "+ str(i)
        try:
            con.execute(sql)
        except:
            pass
    con.close()

    
def distribute_warehouses():
    rand = randint(1,2)
    if rand == 1:
        snowflake_warehouse = 'XSMALL'
    else:
        snowflake_warehouse = 'XSMALL_OVERFLOW'
    return snowflake_warehouse
    

#Used in get_chunk_df() function.  Checks to ensure there are no query chunks > max_rows_per_query_pull in the row_counts, if there is then the function dynamically adjusts query_date_pull_chunk_size_secs (time divide) to ensure there are no gaps in the row_counts when calculating the query chunks
def chunk_check(skipped_chunks, chunk_size, row_counts, time_divide , date_field, database, table_name, cdc_min, con,  loops = 2):
    if skipped_chunks == 0:
        return row_counts, time_divide
    else:
        if time_divide <=1:
            time_divide = 1
        else:
            time_divide = round(time_divide/loops)
        sql = "Select  floor(" + date_field + "/" + str(time_divide) + ") day, count(*) row_count from " + database + '.' + table_name + " WHERE " + date_field + " > " + str(cdc_min) + " group by floor(" + date_field + "/" + str(time_divide) + ") order by floor(" + date_field + "/" + str(time_divide) + ")"
        row_counts = pd.read_sql_query(sql, con)
        row_counts['cumsums'] = row_counts['row_count'].cumsum()
        skipped_chunks = len(row_counts[row_counts['row_count'] > chunk_size])
        if skipped_chunks > 0:
            row_counts, time_divide = chunk_check(skipped_chunks, chunk_size, row_counts, time_divide , date_field, database, table_name, cdc_min, con, loops = loops+1)
        return row_counts, time_divide
    

#creates query chunks based on the cdc date field of the table and creates chunks defined by the "max_rows_per_query_pull" variable.  The "query_date_pull_chunk_size_secs" also affects the behavior of the function by determining how to split up the chunks over a time period that will add up the total rows and get the min and max dates for that total.
#creates query chunks based on the cdc date field of the table and creates chunks defined by the "max_rows_per_query_pull" variable.  The "query_date_pull_chunk_size_secs" also affects the behavior of the function by determining how to split up the chunks over a time period that will add up the total rows and get the min and max dates for that total.
def get_chunk_df( database, table_name, chunk_size = 1000000, time_divide = 86000):
    con = mysql_connection(mysql_username, mysql_password, mysql_hostname, mysql_port)
    snowflake_warehouse = distribute_warehouses()
    sf_con = snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_stage_schema)
    date_field = get_table_date_field(database, table_name, sf_con)
    if date_field == 'NONEXISTENT':
        date_ranges_df  = pd.DataFrame(columns = ['min_date', 'max_date', 'row_count'])
        return date_ranges_df 
    cdc_min = get_cdc_min_date(date_field, database, table_name, 'upload_to_S3_task_', sf_con, use_unix_format = True)
    sql = "Select  floor(" + date_field + "/" + str(time_divide) + ") day, count(*) row_count from " + database + '.' + table_name + " WHERE " + date_field + " > " + str(cdc_min) + " group by floor(" + date_field + "/" + str(time_divide) + ") order by floor(" + date_field + "/" + str(time_divide) + ")"
    row_counts = pd.read_sql_query(sql, con)
    if row_counts.shape[0] == 0:
        max_date = get_cdc_max_date(date_field, database, table_name, con, use_unix_format = True)
        date_ranges_df = pd.DataFrame({'min_date':cdc_min ,'max_date':max_date,'row_count':0}, index=[0])
        date_ranges_df = date_ranges_df.append(pd.DataFrame({'min_date':max_date ,'max_date': max_date ,'row_count':0}, index=[0]), ignore_index = True)
        return date_ranges_df 
    row_counts['cumsums'] = row_counts['row_count'].cumsum()
    skipped_chunks = len(row_counts[row_counts['cumsums'].diff() > chunk_size])
    row_counts, time_divide = chunk_check(skipped_chunks, chunk_size, row_counts, time_divide , date_field, database, table_name, cdc_min, con)
    total_rows_left = sum(row_counts['row_count'])
    l = [n for n in range(0, total_rows_left, chunk_size)]
    l.append(total_rows_left)
    date_ranges_df = pd.DataFrame(columns = ['min_date', 'max_date', 'row_count'])
    for z in range(len(l)-1):
        if len(row_counts[(row_counts.cumsums >= l[z]) & (row_counts.cumsums < l[z+1])]) == 0:
            continue
        if z == 0:
            date_ranges_df.loc[z,'min_date'] = min(row_counts[(row_counts.cumsums >= l[z]) & (row_counts.cumsums <= l[z+1])]['day']) * time_divide
            date_ranges_df.loc[z,'max_date'] = max(row_counts[(row_counts.cumsums >= l[z]) & (row_counts.cumsums <= l[z+1])]['day']) * time_divide
            date_ranges_df.loc[z,'row_count'] = sum(row_counts[(row_counts.cumsums >= l[z]) & (row_counts.cumsums <= l[z+1])]['row_count'])
            date_ranges_df = date_ranges_df.drop_duplicates()
        else:
            date_ranges_df.loc[z,'min_date'] = date_ranges_df.loc[z-1,'max_date']
            date_ranges_df.loc[z,'max_date'] = max(row_counts[(row_counts.cumsums >= l[z]) & (row_counts.cumsums <= l[z+1])]['day']) * time_divide
            date_ranges_df.loc[z,'row_count'] = sum(row_counts[(row_counts.cumsums >= l[z]) & (row_counts.cumsums <= l[z+1])]['row_count']) 
        max_day = max(row_counts['day'])
        max_date = get_cdc_max_date(date_field, database, table_name, con, use_unix_format = True)
        date_ranges_df = date_ranges_df.append(pd.DataFrame({'min_date':max_day * time_divide ,'max_date': max_date ,'row_count':0}, index=[0]), ignore_index = True)
        date_ranges_df = date_ranges_df.append(pd.DataFrame({'min_date':max_date ,'max_date': max_date ,'row_count':0}, index=[0]), ignore_index = True)
        date_ranges_df = date_ranges_df.drop_duplicates()
    if date_ranges_df.shape[0] == 0:
        cdc_max = get_cdc_max_date(date_field, database, table_name, con, use_unix_format = True)
        date_ranges_df.loc[0,'min_date'] = cdc_min
        date_ranges_df.loc[0,'max_date'] = cdc_max
        date_ranges_df.loc[0,'row_count'] = max(row_counts['cumsums'])
        date_ranges_df = date_ranges_df.append(pd.DataFrame({'min_date':cdc_max ,'max_date': cdc_max ,'row_count':0}, index=[0]), ignore_index = True)
        date_ranges_df = date_ranges_df.drop_duplicates()
    con.close()
    sf_con.close()
    gc.collect()
    print(sql)
    return date_ranges_df 


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
    hook = airflow.hooks.S3_hook.S3Hook(s3_connection_id)
    snowflake_warehouse = distribute_warehouses()
    con = mysql_connection(mysql_username, mysql_password, mysql_hostname, mysql_port)
    sf_con = snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_stage_schema)
    date_field = get_table_date_field(database, table_name, sf_con)
    query_chunks_df = get_chunk_df(database, table_name, chunk_size = max_rows_per_query_pull, time_divide = query_date_pull_chunk_size_secs)
    num_chunks = len(query_chunks_df['max_date'])

    #setting table columns
    tbl_def = pd.read_sql_query('describe ' + database + '.' + table_name, con)
    if exclude_columns == True:
        tbl_def = tbl_def[~tbl_def.Field.isin(exclude_cols)]
    tbl_def['replace_state'] = np.where((tbl_def['Type'].str.slice(0,3) == 'var') | (tbl_def['Type'].str.slice(0,3) == 'cha') | (tbl_def['Type'].str.slice(0,3) == 'tex') | (tbl_def['Type'].str.slice(0,3) == 'lon'),'replace(replace(replace(replace(A.'+tbl_def['Field']+", '\\n',''),'\\r',''),'\"',''),'|','\"|\"') AS "+ tbl_def['Field'],"A." + tbl_def['Field'])
    
    
    if num_chunks == 0:
        num_chunks = 1
    for chunk_num in range(0, num_chunks):
        if date_field == 'NONEXISTENT':
            audit_df = start_audit_source_to_s3(Source_System_Name, date_field, database, table_name, Task_id, con, sf_con)
            sql_statement = "SELECT " + tbl_def['replace_state'].str.cat(sep = ', ' ) + " FROM " + database + '.' + table_name + " AS A "
            s3_file_name = generate_s3_file_name(database, table_name)
            local_folder_path = 'tmp/' +  s3_file_name
        else:
            audit_df = start_audit_source_to_s3(Source_System_Name, date_field, database, table_name, Task_id, con, sf_con, cdc_min_unix = query_chunks_df['min_date'][chunk_num], cdc_max_unix = query_chunks_df['max_date'][chunk_num])
            s3_file_name = generate_s3_file_name(database, table_name, cdc_min_unix =  query_chunks_df['min_date'][chunk_num], cdc_max_unix = query_chunks_df['max_date'][chunk_num])
            local_folder_path = 'tmp/' +  s3_file_name
            sql_statement = "SELECT " + tbl_def['replace_state'].str.cat(sep = ', ' ) + " FROM " + database + '.' + table_name + " AS A "
            if audit_df['CDCMIN_UNIX'][0] == audit_df['CDCMAX_UNIX'][0]:
                sql_statement = sql_statement + ' where ' +  date_field + ' = ' + str(audit_df['CDCMAX_UNIX'][0])
            else:
                sql_statement = sql_statement + ' where ' +  date_field + ' >= ' + str(audit_df['CDCMIN_UNIX'][0]) + ' and ' + date_field +  ' <' + str(audit_df['CDCMAX_UNIX'][0])
            
            
        
        

        if mysql_direct_to_file == True:
            sql_statement = sql_statement + " INTO OUTFILE '" + local_folder_path + "' FIELDS TERMINATED BY '" + file_delim + "' LINES TERMINATED BY '\r\n'"  
            con.execute(sql_statement)
            hook.load_file(local_folder_path, s3_file_name, bucket_name)
            os.remove(local_folder_path)
            
        else:
            file_count = 1
            row_count = 0
            for file_chunk in pd.read_sql_query(sql_statement, con, chunksize = max_rows_per_text_file):
                mysql_results = file_chunk.to_csv(sep = file_delim, index = False)
                s3_file_name = 'snowflake/' + generate_s3_file_name(database, table_name, file_iterator = file_count, cdc_min_unix = audit_df['CDCMIN_UNIX'][0], cdc_max_unix = audit_df['CDCMAX_UNIX'][0]) #
                hook.load_string(mysql_results, s3_file_name, bucket_name, replace = True)
                row_count += file_chunk.shape[0]
                file_count += 1
    
        end_audit(database, table_name, sf_con, Task_id,  inserted_rows = row_count, sf_sql_statement = None)
    
    #kill_zombie_connections(con)
    con.close()
    sf_con.close()
    gc.collect()



def upload_to_snowflake(database, table_name, Task_id, Prev_task_id, field_delim = '|'):
    sf_table = database + '.' + table_name
    s3_file_base_name = 'snowflake/' +  database + '_' + table_name
    snowflake_warehouse = distribute_warehouses()
    con =  snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_stage_schema)
    #audit_table = distribute_audit_inserts()
    audit_df = start_audit_snowflake(database, table_name, Task_id, Prev_task_id, con)
    
    #getting list of files
    hook = airflow.hooks.S3_hook.S3Hook(s3_connection_id)
    logging.info('SETTING CHECK AFTER HOOK!!!')
    file_list = hook.list_keys(bucket_name = s3_bucket_name, prefix = s3_file_base_name) #hook.list_keys(bucket_name = s3_bucket_name) 
    logging.info('SETTING CHECK AFTER PULLING FILE LIST!!!')
    if file_list == None: 
        logging.info('NOTHING IN FILE_LIST')
        end_audit(database, table_name, con, Task_id)
        con.close()
        con.close()
        gc.collect()
        return
    file_list_slim = fnmatch.filter(file_list, s3_file_base_name + "_[0-9][0-9][0-9]" + '*')
    file_list_slim.extend(fnmatch.filter(file_list, s3_file_base_name + "_[0-9]_" + '*'))
    logging.info('SETTING CHECK AFTER FILE_LIST_SLIM!!!')
    if len(file_list_slim) == 0:
        logging.info('FILE_LIST_SLIM = 0!!!')
        end_audit(database, table_name, con, Task_id)
        con.close()
        con.close()
        gc.collect()
        return
    else:
        logging.info('FILE_LIST_SLIM > 0!!!')
        file_list_slim = fnmatch.filter(file_list, s3_file_base_name + "_[0-9][0-9][0-9]" + '*')
        file_list_slim.extend(fnmatch.filter(file_list, s3_file_base_name + "_[0-9]_" + '*'))
        #file_list_slim.extend(fnmatch.filter(file_list, s3_file_base_name +'?' + '.txt'))
        file_list_slim_str = str(file_list_slim).strip('[]')
        
        #sql copy into statement
        logging.info('SETTING CHECK AFTER BEFORE CREATING SQL STATEMENT!!!')
        copy = ("copy into %s from '@%s'"
                " files = ( %s )"
                " file_format = (type = csv field_delimiter = '|'"
                " skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = None)"
                % (sf_table, sfstage, file_list_slim_str)
                )
        if audit_df['CDCMIN_UNIX'][0] == 0 and audit_df['CDCMAX_UNIX'][0] == 9223372036:
            logging.info('NO DATE FIELD FOR TABLE, DELETING TABLE!')
            con.execute('Delete from ' +   sf_table)
            copy = copy + " Force = True"
        
        logging.info('SETTING CHECK AFTER BEFORE COPY INTO STATEMENT!!!')
        con.execute(copy)
        logging.info('SETTING CHECK AFTER RUNNING COPY INTO STATEMENT!!!')
        file_list_slim2 = [f.replace('snowflake/', '') for f in file_list_slim]
        
        for i in file_list_slim2:
            hook.copy_object(source_bucket_key = 'snowflake/' + i, dest_bucket_key =  'snowflake/success/' + i, source_bucket_name = s3_bucket_name,  dest_bucket_name = s3_bucket_name)
        logging.info('SETTING CHECK AFTER MOVING FILES!!!')
        hook.delete_objects(bucket = s3_bucket_name, keys = file_list_slim)
        end_audit(database, table_name, con, Task_id, sf_sql_statement = copy)
    con.close()
    #mysql_con = mysql_connection(mysql_username, mysql_password, mysql_hostname, mysql_port)
    #kill_zombie_connections(mysql_con)
    #mysql_con.close()
    con.close()
    gc.collect()





#############################################################################
#Defining SubDag structure
#############################################################################
       
default_args = {
    'owner': 'dale'
    ,'start_date': datetime(2019, 9, 1)
    ,'retry_delay': timedelta(minutes=.25)
    ,'retries': max_task_retries_on_error
}

def database_sub_dag(parent_dag_name, database_name, schedule_interval): #'@once'
    one_dag =  DAG(parent_dag_name + '.' + database_name, default_args=default_args, schedule_interval=schedule_interval, concurrency = 5) #in production, need to update this to run once daily (add various dags and set variables in Airflow?)
    
    #start dummy taks
    start_task = DummyOperator(
        task_id='start_task',
        dag=one_dag
    )
        
    
    # Creates the tasks dynamically.  Each one will elaborate one chunk of data.
    def create_dynamic_task_tos3(table):
        return PythonOperator(
            #provide_context=True,
            task_id='upload_to_S3_task_' + table,
            pool='Pool_max_parallel_5',
            python_callable=upload_table_to_S3_with_hook,
            op_kwargs={		   
            'Source_System_Name': Source_System_Name,
            'database': database_name,
            'Task_id': 'upload_to_S3_task_',
            'bucket_name': s3_bucket_name,
            'table_name': table
            #'exclude_columns': False
            },
            dag=one_dag)
        
    def create_dynamic_task_tosf(table):
        return PythonOperator(
            #provide_context=True,
            task_id='upload_to_snowflake_task_' + table,
            pool='Pool_max_parallel_5',
            python_callable=upload_to_snowflake,
            op_kwargs={
    		'database': database_name,
            'table_name': table,
            'Task_id': 'upload_to_snowflake_task_',
            'Prev_task_id': 'upload_to_S3_task_'   
            },
            dag=one_dag)
        
    #end dummy dag
    end = DummyOperator(
        task_id='end',
        dag=one_dag)
    
    
    
    tbl_list = get_table_list(database_name, exclude_tables = True, exclude_tbls_list = excluded_tables) #collecting all table names from database database
    
    #Setting dependencies, the configuration below creates a parallel task for each table  that migrates the table from mysql to s3, then from s3 to 
    for t in tbl_list:
        dt_s3 = create_dynamic_task_tos3(t)
        dt_sf = create_dynamic_task_tosf(t)
        start_task >> dt_s3
        dt_s3 >> dt_sf
        dt_sf >> end
    
    return one_dag


#############################################################################
#Defining Main Dag structure
#############################################################################

 

main_dag = DAG(
    dag_id=parent_dag_name
    ,default_args=default_args
    ,schedule_interval='@once'
    #schedule_interval=timedelta(minutes=5),
    #max_active_runs=1
    ,concurrency = 10
)


database_list = get_database_list(database_include_patterns)




#Each database is an independant task that will run in parallel4
for i in database_list:
    sub_dag = SubDagOperator(
        subdag = database_sub_dag(parent_dag_name, i, '@once'),
        task_id= i,
        dag=main_dag,
        pool='Pool_max_parallel_5',
        executor=LocalExecutor()
    )



