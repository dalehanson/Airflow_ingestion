# -*- coding: utf-8 -*-
"""
Created on Mon Jan 20 08:09:53 2020

@author: dale.hanson
"""


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
from airflow.models import TaskInstance as task


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

parent_dag_name = 'xcom_test'

database_include_patterns = ['trans*'] #only inlcude the staging, transaction, and gateway databases, for multiple format as a list seperated by commas

excluded_tables = ['web_service_request_log_part', 'web_service_request_log','web_service_request_attribute' 'event_queue_id', 'ct_config_version_cache', 'event_queue_listener_log', 'web_service_request_log_part_old', 'web_service_request_log_old','web_service_request_attribute_old' 'event_queue_id_old', 'ct_config_version_cache_old', 'event_queue_listener_log_old']  #list of tables we dont want to migrate

max_task_time = int(Variable.get('set_task_max_time_minutes')) #set the max runtime for a task
max_task_retries_on_error = int(Variable.get('max_task_retries_on_error'))

mysql_con = BaseHook.get_connection('mysql_celltrak_1') #Airflow_mysql_connection_name
mysql_username = mysql_con.login 
mysql_password = mysql_con.password 
mysql_hostname = mysql_con.host
mysql_port = mysql_con.port


#Create Connection functions
def mysql_connection(username, password, hostname, port):
    engine = create_engine('mysql://' + username + ':' + password + '@' + hostname + ':' + str(port) + '/')
    con = engine.connect()
    return con

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

########################################################################
#Defining main Airflow Task functions (used in main dag)
########################################################################
    

def distribute_audit_inserts():
    rand = randint(1,2)
    if rand == 1:
        audit_table = 'AUDIT_TASK_TABLE'
    else:
        audit_table = 'AUDIT_TASK_TABLE_OVERFLOW'
    return audit_table
    
    
    
def upload_table_to_S3_with_hook(Source_System_Name, bucket_name, database, table_name, Task_id, file_delim = '|', mysql_direct_to_file = False , exclude_columns = False, exclude_cols = [], **kwargs):
    #context = task().get_template_context()
    #t = task()
    #audit_table = kwargs['ti'].xcom_pull(task_ids='select_audit_table_' + database + '_' + table_name)
    audit_table = kwargs['ti'].xcom_pull(task_ids='select_audit_table_' + database + '_' + table_name)
    logging.info('selected table: ' + audit_table)
    



def upload_to_snowflake(database, table_name, Task_id, Prev_task_id, field_delim = '|', **kwargs):
    #t = task()
    audit_table = kwargs['ti'].xcom_pull(task_ids='select_audit_table_' + database + '_' + table_name)
    logging.info('selected table: ' + audit_table)





#############################################################################
#Defining SubDag structure
#############################################################################
    
default_args = {
    'owner': 'dale'
    ,'start_date': datetime(2019, 9, 1)
    ,'retry_delay': timedelta(minutes=.25)
    #,'concurrency': 22
}


one_dag =  DAG(parent_dag_name, default_args=default_args, schedule_interval='@once') #in production, need to update this to run once daily (add various dags and set variables in Airflow?)

#end dummy dag
start_task = DummyOperator(
    task_id='start_task',
    dag=one_dag
)


# Dynamcially creates a task that randomly selects which audit table to insert data into with the goal of distibuting inserts which overcomes snowflake locking table issues
def create_dynamic_task_dist_audit(database_name,table):
    return PythonOperator(
        #provide_context=True,
        task_id='select_audit_table_' + database_name + '_' + table,
        pool='Pool_max_parallel_5',
        python_callable=distribute_audit_inserts,
        dag=one_dag)
    
# Creates the tasks dynamically.  Each one will elaborate one chunk of data.
def create_dynamic_task_tos3(database_name,table):
    return PythonOperator(
        provide_context=True,
        task_id='upload_to_S3_task_' + database_name + '_' + table,
        pool='Pool_max_parallel_5',
        python_callable=upload_table_to_S3_with_hook,
        op_kwargs={		   
        'Source_System_Name': Source_System_Name,
        'database': database_name,
        'Task_id': 'upload_to_S3_task_',
        'bucket_name': s3_bucket_name,
        'table_name': table,
        #'exclude_columns': False
        },
        dag=one_dag)

      
def create_dynamic_task_tosf(database_name,table):
    return PythonOperator(
        provide_context=True,
        task_id='upload_to_snowflake_task_' + database_name + '_' + table,
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
    dag=one_dag
)
    
database_list = ['transaction_strhhs', 'transaction_sufolk'] # 


#Setting dependencies, the configuration below creates a parallel task for each table  that migrates the table from mysql to s3, then from s3 to 
for i in database_list:
    tbl_list = get_table_list(i, exclude_tables = True, exclude_tbls_list = excluded_tables) 
    for t in tbl_list[0:2]:
        if 'combined_list' in locals():
            combined_list.extend([[i,t]])
        else:
            combined_list = [[i,t]]

#collecting all table names from database database
for c in combined_list:
    audit_select = create_dynamic_task_dist_audit(c[0],c[1])
    dt_s3 = create_dynamic_task_tos3(c[0],c[1])
    dt_sf = create_dynamic_task_tosf(c[0],c[1])
    start_task >> audit_select
    audit_select >> dt_s3
    dt_s3 >> dt_sf
    dt_sf >> end
    



