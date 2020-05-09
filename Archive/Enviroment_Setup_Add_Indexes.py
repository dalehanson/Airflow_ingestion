# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 15:35:13 2020

@author: dale.hanson
"""

from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator, SubDagOperator
from airflow.executors.local_executor import LocalExecutor
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from airflow.models import Variable
import pandas as pd
import fnmatch
import logging


parent_dag_name = 'Enviroment_Setup_Add_Indexes'

Airflow_snowflake_connection_name = Variable.get('Airflow_snowflake_connection_name')
Airflow_mysql_connection_name = Variable.get('Airflow_mysql_connection_name')
orchestration_country = Variable.get('orchestration_country')
excluded_databases = Variable.get('excluded_databases')

database_include_patterns = ['trans*'] #only inlcude the staging, transaction, and gateway databases, for multiple format as a list seperated by commas

include_tables = Variable.get('Migration_tables_to_include')
include_tables = include_tables.split(',')
include_tables = [x.strip(' ').lower() for x in include_tables]
#['activity', 'activity_alert','activity_member',  'activity_template_schedule', 'activity_filter', 'activity_template', 'activity_type', 'alert', 'filter', 'filter_group', 'member', 'member_contact', 'member_location', 'activity_travel', 'activity_form_response', 'activity_type_config', 'alert_config', 'member_location_address', 'member_filter', 'member_filter_attribute', 'string_translation', 'device', 'member_device', 'member_address','member_attribute','member_device', 'activity_event','member_location_log','activity_manual_intervention','field_option', 'string_translation', 'activity_export']




mysql_con = BaseHook.get_connection('mysql_celltrak_crd')
mysql_username = mysql_con.login 
mysql_password = mysql_con.password 
mysql_hostname = mysql_con.host
mysql_port = mysql_con.port

sf_con_parm = BaseHook.get_connection(Airflow_snowflake_connection_name) 
snowflake_username = sf_con_parm.login 
snowflake_password = sf_con_parm.password 
snowflake_account = sf_con_parm.host 
snowflake_stage_schema = 'A_UTILITY' 
snowflake_warehouse = "MYSQL_TO_RAW_MIGRATION_XSMALL_1" 
if orchestration_country.lower() in ['us', 'usa','united states','u.s.','u.s.a']:
    snowflake_database = "US_RAW"
if orchestration_country.lower() in ['ca', 'canada','c.a.']:
    snowflake_database = "CA_RAW"
if orchestration_country.lower() in ['uk', 'u.k.','united kingdom']:
    snowflake_database = "UK_RAW"



#Create Connection functions
def mysql_connection(username, password, hostname, port):
    engine = create_engine('mysql://' + username + ':' + password + '@' + hostname + ':' + str(port) + '/')
    con = engine.connect()
    return con


def snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_stage_schema = ""):
    engine2 = create_engine('snowflake://' + str(snowflake_username) + ":" + str(snowflake_password) +'@' + str(snowflake_account) + '/' + str(snowflake_database) + '/' + str(snowflake_stage_schema) + '?' + 'warehouse=' + str(snowflake_warehouse))
    sf_con = engine2.connect()
    return sf_con


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


def add_index_to_tbl(database, table):
    index_list_sql ="Select 'ALTER TABLE ' || table_schema || '.' || table_name || ' ADD INDEX `' || CDC_DATE_FIELD || '` (`' || CDC_DATE_FIELD || '`)' AS ADD_INDEX from " + snowflake_database + ".A_UTILITY.DATE_FIELD_REFERENCE where table_schema ='" + database.upper() + "' and table_name = '" + table.upper() +"'"
    con = mysql_connection(mysql_username, mysql_password, mysql_hostname, mysql_port)
    sf_con = snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_stage_schema = snowflake_stage_schema)
    index_list = pd.read_sql(index_list_sql, sf_con)
    stat = index_list['add_index'][0]
    stat = stat.lower()
    try:
        con.execute(stat)
    except:
        logging.info('Index already exists!')

###############################################
#Substructure
################################################
        
default_args = {
    'owner': 'dale'
    ,'start_date': datetime(2019, 9, 1)
    ,'retry_delay': timedelta(minutes=.25)
    ,'retries': 1
}

def database_sub_dag(parent_dag_name, database_name, schedule_interval): #'@once'
    one_dag =  DAG(parent_dag_name + '.' + database_name, default_args=default_args, schedule_interval=schedule_interval, concurrency = 50, catchup = False)
    
    #start dummy taks
    start_task = DummyOperator(
        task_id='start_task',
        dag=one_dag
    )

    # Creates the tasks dynamically.  Each one will elaborate one chunk of data.
    def create_dynamic_task_add_primary_key(table):
        return PythonOperator(
            #provide_context=True,
            task_id='Add_primary_key_for_' + database_name + '_' + table,
            pool='Pool_max_parallel_5',
            python_callable=add_index_to_tbl,
            op_kwargs={
            'database':  database_name,
            'table': table,
            },
        dag=one_dag)
        
    #end dummy dag
    end = DummyOperator(
        task_id='end',
        dag=one_dag
        )
    
    #tbl_list = get_table_list(database_name, include_tables) #collecting all table names from database database
    tbl_list = ['member_type', 'member_device_state', 'member_role', 'string_translation', 'activity_event', 'activity_manual_intervention', 'role', 'role_entity_permission', 'field_option', 'member_filter_attribute', 'activity_alert_status', 'member_location_log', 'activity_export', 'activity']

    #Setting dependencies, the configuration below creates a parallel task for each table  that migrates the table from mysql to s3, then from s3 to 
    for t in tbl_list:
        dt_cts = create_dynamic_task_add_primary_key(t)
        start_task >> dt_cts
        dt_cts >> end
    
    return one_dag


#############################################################################
#Defining Main Dag structure
#############################################################################

#getting list of all wanted databases
database_list =  get_database_list(trim_by_patterns = database_include_patterns, excluded_databases = excluded_databases)

main_dag = DAG(
    dag_id=parent_dag_name
    ,default_args=default_args
    ,schedule_interval='@once'
    #schedule_interval=timedelta(minutes=5),
    #max_active_runs=1
    ,concurrency = 3
    ,catchup = False
)

start_task = DummyOperator(
        task_id='start_task',
        dag=main_dag
)
    

    #end dummy dag  
end = DummyOperator(
    task_id='end',
    dag=main_dag
)


    


#Each database is an independant task that will run in parallel
def subdag_task(database):
    sub_dag = SubDagOperator(
        subdag = database_sub_dag(parent_dag_name, database, '@once'),
        task_id= database,
        dag=main_dag,
        pool='Pool_max_parallel_500',
        executor=LocalExecutor()
    )
    return sub_dag


for i in database_list:
    sbt = subdag_task(i)
    start_task >> sbt
    sbt >> end
