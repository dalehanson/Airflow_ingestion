

from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from datetime import datetime, timedelta
import Migration_All_Databases_Mysql_to_Snowflake_functions as functs

##################################################################
#Setting variable definitions and connections
##################################################################

parent_dag_name = 'Enviroment_Setup_Create_All_Empty_Customer_Database_Tables_in_Snowflake'

Airflow_snowflake_connection_name = Variable.get('Airflow_snowflake_connection_name')
Airflow_mysql_connection_name = Variable.get('Airflow_mysql_connection_name')
orchestration_country = Variable.get('orchestration_country')

database_include_patterns = ['prefix*'] #only inlcude databases that start with "prefix", for multiple format as a list seperated by commas

excluded_databases = Variable.get('excluded_databases')

modified_on_field_name = Variable.get('modified_on_field_name')
added_on_field_name = Variable.get('added_on_field_name')

##################################################################
#Collection Connection attributes from Airflow connections repo
##################################################################


sf_con = BaseHook.get_connection(Airflow_snowflake_connection_name)
snowflake_username = sf_con.login 
snowflake_password = sf_con.password 
snowflake_account = sf_con.host 
snowflake_database = 'A_UTILITY'
snowflake_warehouse = "XSMALL" 



mysql_con = BaseHook.get_connection(Airflow_mysql_connection_name)
mysql_username = mysql_con.login 
mysql_password = mysql_con.password 
mysql_hostname = mysql_con.host
mysql_port = mysql_con.port





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


database_list = functs.get_database_list(mysql_username, mysql_password, mysql_hostname, mysql_port, trim_by_patterns = database_include_patterns, excluded_databases = excluded_databases) 

#start dummy task
start = DummyOperator(
    task_id='start',
    dag=one_dag)


# Creates the tasks dynamically.  Each one will elaborate one chunk of data.
create_tables_for_multiple_dbs = PythonOperator(
        #provide_context=True,
        task_id='create_tables_for_multiple_dbs',
        pool='Pool_max_parallel_5',
        python_callable=functs.generate_sf_multiple_db_tables,
        op_kwargs={
        'db_list': database_list,
        'mysql_username': mysql_username,
        'mysql_password': mysql_password,
        'mysql_hostname': mysql_hostname,
        'mysql_port': mysql_port,
        'snowflake_username': snowflake_username,
        'snowflake_password': snowflake_password,
        'snowflake_account': snowflake_account,
        'snowflake_database': snowflake_database,
        },
        dag=one_dag)


create_date_references_task_multi_dbs = PythonOperator(
        task_id='load_cdc_date_field_references_mulit_dbs',
        python_callable=functs.load_cdc_date_field_references_multi_dbs,
        op_kwargs={
        'db_list': database_list,
        'modified_on_field_name': modified_on_field_name,
        'added_on_field_name': added_on_field_name,
        'snowflake_username': snowflake_username,
        'snowflake_password': snowflake_password,
        'snowflake_account': snowflake_account,
        'snowflake_database': snowflake_database,
        },
        dag=one_dag)


#end dummy dag
end = DummyOperator(
    task_id='end',
    dag=one_dag)




#Setting dependencies, the configuration below creates a parallel task for each table  that migrates the table from mysql to s3, then from s3 to 


start  >> create_tables_for_multiple_dbs
create_tables_for_multiple_dbs >> create_date_references_task_multi_dbs
create_date_references_task_multi_dbs >> end
