# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 10:10:15 2019

@author: dale.hanson
"""

#This script takes dynamically selects which field to use for cdc date, based on if it has a modified date, added on date or neither.  


from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import fnmatch
import pandas as pd
import gc


##################################################################
#Setting variable definitions
##################################################################

parent_dag_name = 'Create_CDC_Field_Reference_for_Customer_Database'

Airflow_snowflake_connection_name = Variable.get('Airflow_snowflake_connection_name')
orchestration_country = Variable.get('orchestration_country')
max_task_time = int(Variable.get('set_task_max_time_minutes')) #set the max runtime for a task
max_task_retries_on_error = int(Variable.get('max_task_retries_on_error'))


database_include_patterns = ['prefix*'] #only inlcude the staging, transaction, and gateway databases, for multiple format as a list seperated by commas


include_tables = ['table1', 'table2'] 



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


########################################################################
#Defining Utility functions
########################################################################

def snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_schema = ""):
    engine2 = create_engine('snowflake://' + str(snowflake_username) + ":" + str(snowflake_password) +'@' + str(snowflake_account) + '/' + str(snowflake_database) + '/' + str(snowflake_schema) + '?' + 'warehouse=' + str(snowflake_warehouse))
    sf_con = engine2.connect()
    return sf_con


########################################################################
#Defining main Airflow Task functions (used in main dag)
########################################################################

def load_cdc_date_field_references(database, include_tables = None):
    sf_con = snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_schema = snowflake_schema)
    #Collect all tables for a customer database.  this is used as the base for the date reference
    if include_tables is not None:
        include_tables = str(include_tables).strip('[]')
    sql1 = (" SELECT TABLE_SCHEMA, TABLE_NAME"
        " FROM %s.INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = upper('%s')"
        % (snowflake_database, database)
        )
    if include_tables is not None:
        sql1 = sql1 + " and TABLE_NAME in ("  + include_tables + ")"
    tbl_check = pd.read_sql_query(sql1, sf_con)
    #Collecting all tables with columns names, filtered by only pulling in columns named "added_on" or "modified_on".  this table will likely have duplicates for some tables
    sql2 = (" Select * from %s.INFORMATION_SCHEMA.COLUMNS"
            " where TABLE_SCHEMA = upper('%s')"
            " AND (COLUMN_NAME ILIKE 'modified_on%%' OR COLUMN_NAME ILIKE 'added_on%%')"
            % (snowflake_database, database)
            ) 
    date_field_ref = pd.read_sql_query(sql2, sf_con)
    #Collecting tables that have a modified column on it and setting that as the cdc reference
    date_field_ref_to_insert = date_field_ref[date_field_ref['column_name'] == 'MODIFIED_ON'][['table_schema','table_name','column_name']]
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

def get_database_list(trim_by_patterns = None, excluded_databases = None):
    sf_con = snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_schema = snowflake_schema)
    databases = pd.read_sql_query('show schemas', sf_con)
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
    sf_con.close()
    return db_list

def load_cdc_date_field_references_multi_dbs(db_list):
    for database in db_list:
        load_cdc_date_field_references(database)
    gc.collect()


#############################################################################
#Defining Dag structure
#############################################################################
   
database_list =  get_database_list(trim_by_patterns = database_include_patterns) 
   
default_args = {
    'owner': 'dale',
    #'start_date': datetime.now(),
    'start_date': datetime(2019, 9, 1),
    'execution_timeout': timedelta(minutes=max_task_time),
    'retries': max_task_retries_on_error,
    'retry_delay': timedelta(minutes=.5)
}

one_dag =  DAG(parent_dag_name, default_args=default_args, schedule_interval='@once') #in production, need to update this to run once daily (add various dags and set variables in Airflow?)

#start dummy taks
start_task = DummyOperator(
    task_id='start_task',
    dag=one_dag
)
    


create_date_references_task_multi_dbs = PythonOperator(
        task_id='load_cdc_date_field_references_mulit_dbs',
        python_callable=load_cdc_date_field_references_multi_dbs,
        op_kwargs={
        'db_list': database_list
        },
        dag=one_dag)


#end dummy dag
end = DummyOperator(
    task_id='end',
    dag=one_dag)



#Setting dependencies, the configuration below creates a parallel task for each table  that migrates the table from mysql to s3, then from s3 to 
start_task >> create_date_references_task_multi_dbs
create_date_references_task_multi_dbs >> end
