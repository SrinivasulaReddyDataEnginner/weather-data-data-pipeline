from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators import PythonOperator
import os
from airflow.hooks import PostgresHook
import json
import numpy as np
import psycopg2
from psycopg2 import Error

# defining the default arguments
default_args = {
    'owner':'Ashish',
    'depends_on_past': False,
    'email':['ashish.gh123@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':5,
    'retry_delay':timedelta(minutes=1)
    }


# define the dag, the start date how frequently it starts 
dag = DAG(
    dag_id = 'weather_load_dag',
    default_args=default_args,
    start_date = datetime(2019,9,17),
    schedule_interval = timedelta(minutes = 1440)
)

# First task is to query data from the openweather.org
task1 = BashOperator(
    task_id = 'get_weather',
    dag = dag
)

# Second task is to process the data and load into the database
task2 = PythonOperator(
    task_id = 'transform_data',
	provide_context=True,
    dag = dag
)

# setting dependencies
task1 >> task2
