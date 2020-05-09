# -*- coding: utf-8 -*-
"""
Created on Mon Feb 10 19:56:52 2020

@author: dale.hanson
"""

import pandas as pd
import numpy as np
from datetime import datetime as dt
import datetime
import math
import matplotlib.pyplot as plt
import plotly.express as px
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output



pool_size = 128
concurrency_setting = 180
interval = 60*60
downtime_threshold = .05

df = pd.read_csv('C:/Users/dale.hanson/Desktop/airflow_task_duration_counts_dw.csv')

df['start_date'] = pd.to_datetime(df['start_date'])
df['end_date'] = pd.to_datetime(df['end_date'])
df['queued_dttm'] = pd.to_datetime(df['queued_dttm'])
df['dag_id_master'] = df['dag_id'].str.split('.', expand = True)[0]

dag_ids = []
dag_values = []
for i in df['dag_id_master'].unique():
    dic = {'label':i, 'value':i}
    dag_ids.append(dic)
    dag_values.append(i)

mindate = min(df['start_date'])
maxdate = max(df['end_date'])
tot_secs = (maxdate - mindate).total_seconds()
df['secs_since_exec_start'] = (df['start_date']- mindate)/np.timedelta64(1,'s')
df['secs_since_exec_end'] = (df['end_date']- mindate)/np.timedelta64(1,'s')
df['secs_since_exec_queued'] = (df['queued_dttm']- mindate)/np.timedelta64(1,'s')

keep = ['failed','success','up_for_retry']
t = df[df['state'].isin(keep)]

####################################
#comparing job run times at task level
#####################################
#job_duration_comparison = t.pivot(index = 'task_id', columns='execution_date',values='duration')

#summary(t$duration)
#hist(t$duration, breaks = seq(0,max(t$duration, na.rm = TRUE)+10, by = 10))
#mean(counts$concurrent_tasks)
#mean(counts$tasks_in_pool)


counts = pd.DataFrame(columns=["dag_id","chunk", "concurrent_tasks", "queued_tasks", "chunk_start_datetime","chunk_end_datetime"])

for i in range(math.ceil(tot_secs/interval)+1):
    print(i)
    for j in dag_values:
        t2 = t[t['dag_id_master'] == j]
        concurrent_tasks = t2[(t2['secs_since_exec_start'] >= i*interval) & (t2['secs_since_exec_start'] <=(i+1)*interval)].shape[0] + t2[(t2['secs_since_exec_start']< i*interval) & (t2['secs_since_exec_end'] >=(i+1)*interval)].shape[0]
        queued_tasks = t2[(t2['secs_since_exec_queued']>= i*interval) & (t2['secs_since_exec_queued'] <=(i+1)*interval)].shape[0] + t2[(t2['secs_since_exec_queued']< i*interval) & (t2['secs_since_exec_start'] >=(i+1)*interval)].shape[0]
        chunk_start_datetime = mindate + datetime.timedelta(0,i*interval)
        chunk_end_datetime = mindate + datetime.timedelta(0,(i+1)*interval)
        counts.loc[len(counts)] = [j,i,concurrent_tasks,queued_tasks,chunk_start_datetime,chunk_end_datetime]
        chunk_start_datetime = mindate + datetime.timedelta(0,i*interval)
        chunk_end_datetime = mindate + datetime.timedelta(0,(i+1)*interval)
        counts.loc[len(counts)] = [j,i,concurrent_tasks,queued_tasks,chunk_start_datetime,chunk_end_datetime]


counts['tasks_in_pool'] = counts['concurrent_tasks'] + counts['queued_tasks']
counts['down_times'] = np.where(counts['concurrent_tasks'] <= concurrency_setting*downtime_threshold, counts['concurrent_tasks'], np.nan)

main_df = pd.DataFrame(columns=["Avg Task Time", "Avg Tasks running per Min"])
main_df.loc[len(main_df)] = [t['duration'].mean(),counts['concurrent_tasks'].mean()]

########################################
#creating app dashboard
########################################
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

def generate_table(dataframe, max_rows=10):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))],
        style={
                'text-align': 'center'
            }
    )


app.layout = html.Div([
    html.H1(
        children='Airflow Task Scheduler Dashboard',
        style={
            'textAlign': 'center',
            #'color': colors['text']
        }
    ),
    
    html.Label('Select Dag'),
    dcc.Dropdown(
        options=dag_ids,
        value=dag_values,
        multi=True
    ),
    
    generate_table(main_df),
            
    dcc.Graph(id='test')
])
    
@app.callback(
    Output('test', 'figure'),
    [Input('Select Dag', 'value')])

def update_figure(selected_dags):
    filtered_df = counts[counts['dag_id'].isin(selected_dags)]
    filtered_df = filtered_df.groupby(['chunk',"chunk_start_datetime","chunk_end_datetime"]).agg(sum)
    return {
            'data': [
                dict(
                    x=filtered_df['chunk_start_datetime'],
                    y=filtered_df['concurrent_tasks'],
                    name = 'concurrent_tasks',
                    text='concurrent_tasks',
                    mode='lines',
                    opacity=0.7,
                ) ,
                dict(
                    x=filtered_df['chunk_start_datetime'],
                    y=filtered_df['queued_tasks'],
                    name = 'queued_tasks',
                    text='queued_tasks',
                    mode='lines',
                    opacity=0.7,
                ) 
            ],
            'layout': dict(
                xaxis={'value': "%Y-%b-%d %H:%M:%S s", 'title': 'Date & Time'},
                yaxis={'title': 'Tasks'},
                margin={'l': 40, 'b': 40, 't': 10, 'r': 10},
                legend={'x': 0, 'y': 1},
                hovermode='closest',
                title={
                        'text': "Running vs. Queued Tasks",
                        'y':0.9,
                        'x':0.5,
                        'xanchor': 'center',
                        'yanchor': 'top'}
            )
        }
        

if __name__ == '__main__':
    app.run_server(debug=True)