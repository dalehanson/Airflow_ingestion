from airflow import DAG
from airflow.operators import DummyOperator, PythonOperator, TriggerDagRunOperator, BashOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
from airflow.models import Variable
import Migration_All_Databases_Mysql_to_Snowflake_functions as functs



##################################################################
#Setting variable definitions
##################################################################

parent_dag_name = 'Migration_All_Databases_Mysql_to_Snowflake_Dag'


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

schedule_interval = '45 03 * * *'

database_include_patterns = ['prefix*'] #only inlcude databases that start with "prefix", for multiple format as a list seperated by commas
excluded_databases = Variable.get('excluded_databases')

include_tables = Variable.get('Migration_tables_to_include')
include_tables = include_tables.split(',')
include_tables = [x.strip(' ').lower() for x in include_tables]


max_task_time = int(Variable.get('set_task_max_time_minutes')) #set the max runtime for a task
max_task_retries_on_error = int(Variable.get('max_task_retries_on_error'))

migration_audit_folder_path = Variable.get('migration_audit_folder_path') #audit log files for temp storage during run, will be loaded to audit table in snowflake when job is completed

S3_migration_audit_folder_path = Variable.get('S3_migration_audit_folder_path') #S3 location folder where audit files will be staged in S3 to be loaded into snowflake

cdc_times_and_dates_folder_local_location = Variable.get('cdc_times_and_dates_folder_local_location')

########################################################################
#Defining Connections
########################################################################

    

sf_con_parm = BaseHook.get_connection(Airflow_snowflake_connection_name) 
snowflake_username = sf_con_parm.login 
snowflake_password = sf_con_parm.password 
snowflake_account = sf_con_parm.host 
snowflake_stage_schema = 'A_UTILITY' 
snowflake_database = sf_con_parm.schema

mysql_con = BaseHook.get_connection('Airflow_mysql_connection_name)
mysql_username = mysql_con.login 
mysql_password = mysql_con.password 
mysql_hostname = mysql_con.host
mysql_port = mysql_con.port



#############################################################################
#Defining Dag structure
#############################################################################
    
   
default_args = {
    'owner': 'dale'
    ,'start_date': datetime(2020, 3, 9)
    ,'retry_delay': timedelta(minutes=.25)
    ,'retries': max_task_retries_on_error
}
    
main_dag = DAG(
    dag_id=parent_dag_name
    ,default_args=default_args
    ,schedule_interval=schedule_interval
    #schedule_interval=timedelta(minutes=5),
    #max_active_runs=1
    ,concurrency = 300
    ,catchup = False
)

start_task = DummyOperator(
        task_id='start_task',
        dag=main_dag
)

# Defining task to fetch cdc min dates and datefield references and download them to local instance
cdc_times_to_local_instance = PythonOperator(
    task_id = 'cdc_times_to_local_instance',
    python_callable = functs.cdc_times_to_local_instance,
    op_kwargs={
    'cdc_times_and_dates_folder_local_location': cdc_times_and_dates_folder_local_location,
    'snowflake_username': snowflake_username,
    'snowflake_password': snowflake_password,
    'snowflake_account': snowflake_account,
    'snowflake_database': snowflake_database,
    'snowflake_stage_schema': snowflake_stage_schema
    },
    dag = main_dag
)
        
# Creates the tasks dynamically.  Each one will elaborate one chunk of data.
def create_dynamic_task_tos3(database_name, table):
    return PythonOperator(
        task_id='upload_to_S3_task_' + database_name + '_' + table,
        python_callable=functs.upload_table_to_S3_with_hook_v2,
        pool = 'massive_pool',
        op_kwargs={		   
        'Source_System_Name': Source_System_Name,
        'Task_id': 'upload_to_S3_task_',
        'bucket_name': s3_bucket_name,
        'table_name': table,
        'database': database_name,
        's3_connection_id': s3_connection_id,
        'cdc_times_and_dates_folder_local_location': cdc_times_and_dates_folder_local_location,
        'migration_audit_folder_path': migration_audit_folder_path,
        'mysql_username': mysql_username,
        'mysql_password': mysql_password,
        'mysql_hostname': mysql_hostname,
        'mysql_port': mysql_port,
        'max_rows_per_text_file': max_rows_per_text_file
        },
        trigger_rule="all_success",
        dag=main_dag)
            
def create_dynamic_task_tosf(database_name, table):
    return PythonOperator(
        task_id='upload_to_snowflake_task_' + database_name + '_' + table,
        python_callable=functs.upload_to_snowflake,
        pool = 'massive_pool',
        op_kwargs={
        'table_name': table,
        'Task_id': 'upload_to_snowflake_task_',
        'Prev_task_id': 'upload_to_S3_task_',
        'database': database_name,
        'migration_audit_folder_path': migration_audit_folder_path,
        's3_connection_id': s3_connection_id,
        's3_bucket_name': s3_bucket_name,
        'snowflake_username': snowflake_username,
        'snowflake_password': snowflake_password,
        'snowflake_account': snowflake_account,
        'snowflake_database': snowflake_database,
        'snowflake_stage_schema': snowflake_stage_schema,
        'sfstage': sfstage
        },
        trigger_rule="all_done",
        dag=main_dag)
    
kill_zombie_cons = PythonOperator(
        task_id ='kill_zombie_connections',
        python_callable=functs.kill_zombie_connections,
        op_kwargs={
        'mysql_username': mysql_username,
        'mysql_password': mysql_password,
        'mysql_hostname': mysql_hostname,
        'mysql_port': mysql_port,
        },
        trigger_rule="all_done",
        dag=main_dag
 )

load_audit_records_to_s3 = BashOperator(
    task_id = 'load_audit_records_to_s3',
    bash_command=functs.load_audit_records_to_s3(migration_audit_folder_path, s3_bucket_name, S3_migration_audit_folder_path),
    trigger_rule="all_done",
    dag=main_dag
)

clean_up_audit_records = PythonOperator(
    task_id='clean_up_audit_records',
    python_callable=functs.load_audit_records_to_sf,
    op_kwargs={
    'migration_audit_folder_path': migration_audit_folder_path,
    'S3_migration_audit_folder_path': S3_migration_audit_folder_path,
    's3_connection_id': s3_connection_id,
    's3_bucket_name': s3_bucket_name,
    'snowflake_username': snowflake_username,
    'snowflake_password': snowflake_password,
    'snowflake_account': snowflake_account,
    'snowflake_database': snowflake_database,
    'snowflake_stage_schema': snowflake_stage_schema,
    'sfstage': sfstage
    },
    trigger_rule="all_success",
    dag=main_dag
)

trigger = TriggerDagRunOperator(
	task_id = 'trigger_dagrun',
	trigger_dag_id = 'Migration_All_Databases_Mysql_to_Snowflake_no_subdag_addus',
	trigger_rule = 'all_done',
	dag=main_dag
)

    
    #end dummy dag  
end = DummyOperator(
    task_id='end',
    dag=main_dag
)



database_list = functs.get_database_list(mysql_username, mysql_password, mysql_hostname, mysql_port, trim_by_patterns = database_include_patterns, excluded_databases = excluded_databases) 
database_list.sort()
for i in database_list:
    tbl_list = functs.get_table_list(i,mysql_username, mysql_password, mysql_hostname, mysql_port, include_database_list = include_tables)
    for t in tbl_list:
        if 'combined_list' in locals():
            combined_list.extend([[i,t]])
        else:
            combined_list = [[i,t]]




for c in combined_list:
    dt_s3 = create_dynamic_task_tos3(c[0],c[1])
    dt_sf = create_dynamic_task_tosf(c[0],c[1])
    start_task >> cdc_times_to_local_instance
    cdc_times_to_local_instance >>  dt_s3
    dt_s3 >> dt_sf
    dt_sf >> kill_zombie_cons 
    kill_zombie_cons >> load_audit_records_to_s3
    load_audit_records_to_s3 >> clean_up_audit_records
    clean_up_audit_records >> trigger
    trigger >> end
