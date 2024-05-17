from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import XCom
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from datetime import datetime
import os, glob
import random
import great_expectations as gx
import shutil
import pandas as pd
import numpy as np
from collections import namedtuple
import psycopg2
from psycopg2 import sql
from jinja2 import Environment, FileSystemLoader
from pymsteams import connectorcard
import urllib.parse
import re
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'data_validation'))
from data_validation.__init__ import RAW_DATA_DIRECTORY, REPORT_DIRECTORY, TEAMS_WEBHOOK, DB_PARAMS, GOOD_DATA_DIRECTORY, BAD_DATA_DIRECTORY, INDEX_SITE
from data_validation.check_expectations import read_data, validate_data, send_alerts, save_data_errors, save_file

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 27),
    'concurrency': 1,
    'retries': 0
}

dag = DAG(
    'validation_job',
    default_args=default_args,
    description='A DAG to check data expectations',
    schedule_interval="*/1 * * * *",
    catchup = False,
)

read_data_task = PythonOperator(
    task_id='read_data',
    python_callable=read_data,
    op_kwargs={'raw_data_directory': RAW_DATA_DIRECTORY},
    dag=dag
)

validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    op_kwargs={'file_path': "{{ task_instance.xcom_pull(task_ids='read_data') }}"},
    dag=dag
)

send_alerts_task = PythonOperator(
    task_id='send_alerts',
    python_callable=send_alerts,
    op_kwargs={'total_expectations': "{{ task_instance.xcom_pull(task_ids='validate_data', key='return_value')[0] }}",
            'successful_expectations': "{{ task_instance.xcom_pull(task_ids='validate_data', key='return_value')[1] }}",
            'failed_expectations': "{{ task_instance.xcom_pull(task_ids='validate_data', key='return_value')[2] }}",
            'percentage': "{{ task_instance.xcom_pull(task_ids='validate_data', key='return_value')[3] }}",
            'report_directory': REPORT_DIRECTORY,
            'encoded_report_link': INDEX_SITE, #"{{ task_instance.xcom_pull(task_ids='validate_data', key='return_value')[4] }}",
            'teams_webhook': TEAMS_WEBHOOK},
    dag=dag
)

save_data_errors_task = PythonOperator(
    task_id='save_data_errors_to_postgres',
    python_callable=save_data_errors,
    provide_context=True,
    dag=dag
)

save_file_task = PythonOperator(
    task_id='save_file',
    python_callable=save_file,
    op_kwargs={'good_data_directory': GOOD_DATA_DIRECTORY,
            'bad_data_directory': BAD_DATA_DIRECTORY,
            'success_ratio': "{{ task_instance.xcom_pull(task_ids='validate_data', key='return_value')[6] }}",
            'flag': "{{ task_instance.xcom_pull(task_ids='validate_data', key='return_value')[7] }}",
            'rows': "{{ task_instance.xcom_pull(task_ids='validate_data', key='return_value')[8] }}",
            'file_path': "{{ task_instance.xcom_pull(task_ids='read_data') }}"},
    dag=dag
)


# Define task dependencies
read_data_task >> validate_data_task >> [send_alerts_task, save_data_errors_task, save_file_task]
