# -*- coding: utf-8 -*-
"""
Created on Mon Feb 24 13:29:05 2020

@author: dale.hanson
"""

from airflow.utils.email import send_email
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


Airflow_snowflake_connection_name = Variable.get('Airflow_snowflake_connection_name')
Airflow_mysql_connection_name = Variable.get('Airflow_mysql_connection_name')
s3_connection_id = Variable.get('s3_connection_name')
s3_bucket_name = Variable.get('s3_main_bucket_name') 
sfstage = Variable.get('snowflake_stage_name') 
strict_chunk_enforcement = Variable.get('strict_chunk_enforcement')
orchestration_country = Variable.get('orchestration_country')
error_email_recipients = Variable.get('error_email_recipients')

max_rows_per_text_file =  int(Variable.get('max_rows_per_text_file'))
max_rows_per_query_pull =  int(Variable.get('max_rows_per_query_pull'))
query_date_pull_chunk_size_secs = int(Variable.get('query_date_pull_chunk_size_secs'))

Source_System_Name = Variable.get('Source_System_Name')

parent_dag_name = 'Migrate_all_databases_mysql_to_snowflake_for_prod_v4'

database_include_patterns = ['trans*'] #only inlcude the staging, transaction, and gateway databases, for multiple format as a list seperated by commas
excluded_databases = Variable.get('excluded_databases')

include_tables = ['activity']# ['activity', 'activity_alert', 'activity_member', 'activity_template_schedule', 'activity_filter', 'activity_template', 'activity_type', 'alert', 'config_group_filter', 'config_group', 'filter', 'filter_group', 'member', 'member_contact', 'member_location', 'activity_travel', 'activity_form_response', 'activity_type_config', 'alert_config', 'member_location_address', 'member_filter', 'member_filter_attribute', 'string_translation', 'device', 'member_device']

max_task_time = int(Variable.get('set_task_max_time_minutes')) #set the max runtime for a task
max_task_retries_on_error = int(Variable.get('max_task_retries_on_error'))


migration_audit_folder_path = 'snowflake/migration_audit_files/' #audit log files for temp storage during run, will be loaded to audit table in snowflake when job is completed




##################################################################
#Collection Connection attributes from Airflow connections repo
##################################################################


    

sf_con_parm = BaseHook.get_connection(Airflow_snowflake_connection_name) 
snowflake_username = sf_con_parm.login 
snowflake_password = sf_con_parm.password 
snowflake_account = sf_con_parm.host 
snowflake_stage_schema = 'A_UTILITY' 
if orchestration_country.lower() in ['us', 'usa','united states','u.s.','u.s.a']:
    snowflake_database = "US_RAW"
if orchestration_country.lower() in ['ca', 'canada','c.a.']:
    snowflake_database = "CA_RAW"
if orchestration_country.lower() in ['uk', 'u.k.','united kingdom']:
    snowflake_database = "UK_RAW"

mysql_con = BaseHook.get_connection(Airflow_mysql_connection_name) #Airflow_mysql_connection_name
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

def snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse, snowflake_stage_schema = ""):
    engine2 = create_engine('snowflake://' + str(snowflake_username) + ":" + str(snowflake_password) +'@' + str(snowflake_account) + '/' + str(snowflake_database) + '/' + str(snowflake_stage_schema) + '?' + 'warehouse=' + str(snowflake_warehouse))
    sf_con = engine2.connect()
    return sf_con


######################

def missing_databases_alert(mysql_database_list, trim_by_patterns = None):
    snowflake_warehouse = 'MYSQL_TO_RAW_MIGRATION_XSMALL_1'
    sf_con = snowflake_connection(snowflake_username , snowflake_password, snowflake_account, snowflake_database, snowflake_warehouse)
    ##################
    #compare source (mysql server) vs. target database (snowflake raw) list
    ##################
    sf_database_df = pd.read_sql_query('show schemas', sf_con)
    sf_database_df['name'] = sf_database_df['name'].str.lower()
    sf_database_list = sf_database_df['name'].str.strip().to_list()
    if trim_by_patterns is not None:
        db_slim_list = []
        for i in trim_by_patterns:
            db_slim_list.extend(fnmatch.filter(sf_database_list, i))
        sf_database_list = db_slim_list
    sf_missing_databases = [x for x in mysql_database_list if x not in sf_database_list]
    sf_missing_databases_str = str(sf_missing_databases).strip('[]')
    
    ##################
    #send email if missing databases
    ##################
    if len(sf_missing_databases) > 0:
        title = "Airflow alert: Missing Databases Detected in Snowflake!"
        body = ("""
        Warning, new databases dectected in the mysql server enviroment that does not exist in Snowflake.  The detailed list is as follows: <br>
        <br>
        %s <br>
        <br>
        To add addional databases, you will need to refer to the documentation: https://celltrak.atlassian.net/wiki/spaces/PROD/pages/830963809/Project+Documentation  <br>
        <br>
        If you do not want to add these databases to Snowflake, you can avoid future emails by adding the database to the 'excluded_databases' variable in the Airflow UI.
        """ %(sf_missing_databases_str)
        )
        send_email(error_email_recipients,title,body)
    

    