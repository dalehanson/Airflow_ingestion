# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 12:05:25 2019

@author: dale.hanson
"""

#This process will populate a table in snowflake that tracks total row count for each table.  The table is used as a quality check that ensures all data is migrated over to snowflake

from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import gc


##################################################################
#Setting variable definitions and connections
##################################################################

excluded_tables = ['table1','table2']

database_list = ['database']

#database = Variable.get('Create_customer_database_tables_var__database_name')
Airflow_snowflake_connection_name = Variable.get('Airflow_snowflake_connection_name')
Airflow_mysql_connection_name = Variable.get('Airflow_mysql_connection_name')


parent_dag_name = 'Collect_Mysql_Table_Counts_Load_to_Snowflake_v2_quick'

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
    

def get_mysql_database_counts(database):
    table_list = get_table_list(database, exclude_tables = True, exclude_tbls_list = excluded_tables)
    for table in table_list:
        get_mysql_table_counts(database, table)
    gc.collect()
    

def get_multiple_database_counts(database_list):
    for database in database_list:
        get_mysql_database_counts(database)
    gc.collect()
    

#############################################################################
#Defining Dag structure
#############################################################################
    
default_args = {
    'owner': 'dale',
    #'start_date': datetime.now(),
    'start_date': datetime(2019, 9, 1),
    'retry_delay': timedelta(minutes=.5)
    
}

one_dag =  DAG(parent_dag_name, default_args=default_args, schedule_interval='@once') #in production, need to update this to run once daily (add various dags and set variables in Airflow?)

#start dummy taks
start_task = DummyOperator(
    task_id='start_task',
    dag=one_dag
)
    

# Creates the tasks dynamically.  Each one will elaborate one chunk of data.
collect_table_counts = PythonOperator(
        #provide_context=True,
        task_id='Get_mysql_table_counts',
        pool='Pool_max_parallel_5',
        python_callable=get_multiple_database_counts,
        op_kwargs={
        'database_list': database_list,
        },
        dag=one_dag)

    
#end dummy dag
end = DummyOperator(
    task_id='end',
    dag=one_dag)




#Setting dependencies, the configuration below creates a parallel task for each table  that migrates the table from mysql to s3, then from s3 to 
start_task >> collect_table_counts
collect_table_counts >> end