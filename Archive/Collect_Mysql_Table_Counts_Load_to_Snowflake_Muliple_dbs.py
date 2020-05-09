# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 12:05:25 2019

@author: dale.hanson
"""

#This process will populate a table in snowflake that tracks total row count for each table.  The table is used as a quality check that ensures all data is migrated over to snowflake

from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator, SubDagOperator
from airflow.executors.local_executor import LocalExecutor
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import gc


##################################################################
#Setting variable definitions and connections
##################################################################

#database = Variable.get('Create_customer_database_tables_var__database_name')
Airflow_snowflake_connection_name = Variable.get('Airflow_snowflake_connection_name')
Airflow_mysql_connection_name = Variable.get('Airflow_mysql_connection_name')

database_list = ['database']

parent_dag_name = 'Collect_Mysql_Table_Counts_Load_to_Snowflake_Muliple_dbs'

sf_con = BaseHook.get_connection(Airflow_snowflake_connection_name)
snowflake_username = sf_con.login 
snowflake_password = sf_con.password 
snowflake_account = sf_con.host 
snowflake_warehouse = "XSMALL" 
snowflake_database = "sf_db"

mysql_con = BaseHook.get_connection(Airflow_mysql_connection_name)
mysql_username = mysql_con.login 
mysql_password = mysql_con.password 
mysql_hostname = mysql_con.host
mysql_port = mysql_con.port

########################################################################
#Defining Utility functions
########################################################################

#Create Connection functions
def mysql_connection(username, password, hostname, port):
    engine = create_engine('mysql://' + username + ':' + password + '@' + hostname + ':' + str(port) + '/')
    con = engine.connect()
    return con

def snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse):
    engine2 = create_engine('snowflake://' + snowflake_username + ":" + snowflake_password +'@' + snowflake_account + '/' + snowflake_database + '/' + '?' + 'warehouse=' + snowflake_warehouse, echo = False)
    sf_con = engine2.connect()
    return sf_con

def get_table_list(database):
    engine = create_engine('mysql://' + mysql_username + ':' + mysql_password + '@' + mysql_hostname + ':' + str(mysql_port) + '/')
    con = engine.connect()
    sql_statement =  'Show tables from ' + database
    tbls = pd.read_sql_query(sql_statement, con)
    tbls.columns = ['table']  
    tbls['table'] = tbls['table'].str.strip()
    tbls = tbls['table'].to_list()
    con.close()
    gc.collect()
    return tbls

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
########################################################################
#Defining main Airflow Task functions (used in main dag)
########################################################################


def get_mysql_table_counts(database, table):
    con = mysql_connection(mysql_username, mysql_password, mysql_hostname, mysql_port)
    sf_con = snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse)
    sql = ("select UPPER('%s') AS 'TABLE_SCHEMA', UPPER('%s') AS 'TABLE_NAME', count(*) AS 'ROW_COUNT', NOW() AS 'DATETIME_RECORDED' from %s.%s" 
    % (database, table ,database, table)
          )
    tbl_count = pd.read_sql_query(sql, con)
    tbl_count.to_sql('SOURCE_TABLE_COUNT_RECORD', schema = 'A_UTILITY', con = sf_con, if_exists = 'append', index = False)
    kill_zombie_connections(con)
    con.close()
    sf_con.close()
    gc.collect()
    



#############################################################################
#Defining SubDag structure
#############################################################################


default_args = {
    'owner': 'dale'
    ,'start_date': datetime(2019, 9, 1)
    ,'retry_delay': timedelta(minutes=.25)
    ,'retries': 3 #max_task_retries_on_error
    ,'concurrency': 20
}

def database_sub_dag(parent_dag_name, database_name, schedule_interval): #'@once'
    one_dag =  DAG(parent_dag_name + '.' + database_name, default_args=default_args, schedule_interval=schedule_interval) #in production, need to update this to run once daily (add various dags and set variables in Airflow?)
    
    #start dummy taks
    start_task = DummyOperator(
        task_id='start_task',
        dag=one_dag
    )

    

    # Creates the tasks dynamically.  Each one will elaborate one chunk of data.
    def create_dynamic_task_collect_table_counts(table):
        return PythonOperator(
            #provide_context=True,
            task_id='Get_mysql_table_counts_for_' + table,
            pool='Pool_max_parallel_5',
            python_callable=get_mysql_table_counts,
            op_kwargs={
            'database':  database_name,
            'table': table,
            },
            dag=one_dag)

    
    #end dummy dag
    end = DummyOperator(
        task_id='end',
        dag=one_dag)



    tbl_list = get_table_list(database_name) #collecting all table names from database

    #Setting dependencies, the configuration below creates a parallel task for each table  that migrates the table from mysql to s3, then from s3 to 
    for t in tbl_list:
        dt_create_tables = create_dynamic_task_collect_table_counts(t)
        start_task >> dt_create_tables
        dt_create_tables >> end
    
    return one_dag

#############################################################################
#Defining Main Dag structure
#############################################################################


main_dag = DAG(
    dag_id=parent_dag_name,
    default_args=default_args,
    schedule_interval='@once'
    #schedule_interval=timedelta(minutes=5),
    #,max_active_runs=35
    ,concurrency=20
)


#Each database is an independant task that will run in parallel
for i in database_list:
    sub_dag = SubDagOperator(
        subdag = database_sub_dag(parent_dag_name, i, '@once'),
        task_id= i,
        dag=main_dag,
        executor=LocalExecutor()
    )




