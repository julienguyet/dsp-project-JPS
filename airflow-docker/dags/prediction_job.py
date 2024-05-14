from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.hooks.http_hook import HttpHook
from datetime import datetime
import os
import requests
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'data_validation'))
from data_validation.__init__ import GOOD_DATA_DIRECTORY

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 6),
    'concurrency': 1,
    'retries': 0
}

data_folder = GOOD_DATA_DIRECTORY
fastapi_url = "/predict/"

def check_files(data_folder):
    record_file_path = os.path.join(data_folder, "processed_files.txt")
    if not os.path.exists(record_file_path):
        open(record_file_path, 'w').close()

    with open(record_file_path, 'r') as record_file:
        processed_files = record_file.read().splitlines()

    files_in_folder = os.listdir(data_folder)

    new_files = [file for file in files_in_folder if file not in processed_files and file !=".DS_Store" and file!="processed_files.txt"]

    if new_files:
        with open(record_file_path, 'a') as record_file:
            record_file.write("\n".join(new_files))
            record_file.write("\n")
        return new_files
    else:
        return False

'''def upload_files(**kwargs):
    task_instance = kwargs['task_instance']
    new_files = task_instance.xcom_pull(task_ids='check_for_new_data')
    if new_files:
        print("Processing new files:", new_files)
        for file in new_files:
            filepath = os.path.join(data_folder, file)
            hook = HttpHook(method='POST', http_conn_id='http_conn_id')
            response = hook.run(endpoint=fastapi_url, data={'filepath': filepath})
            if response.status_code == 200:
                print("New files forwarded to FastAPI successfully")
            else:
                print("Failed to forward new files to FastAPI:", response.text)
    else:
        print("No new files to process")'''

def upload_files(**kwargs):
    task_instance = kwargs['task_instance']
    new_files = task_instance.xcom_pull(task_ids='check_for_new_data')
    if new_files:
        print("Processing new files:", new_files)
        file_paths = [os.path.join("/Users/julien/Documents/EPITA/S2/DSP/dsp-project-JPS/airflow-docker/dags/good_data", file) for file in new_files]
        # Send file paths in request body
        hook = HttpHook(method='POST', http_conn_id='http_conn_id')
        response = hook.run(endpoint=fastapi_url, json=file_paths)
        # Handle response as needed
    else:
        print("No new files to process")

with DAG(
    'prediction_job',
    default_args=default_args,
    description='A DAG to check for new files in the data folder and upload them to FastAPI',
    schedule_interval="*/2 * * * *",
    catchup=False,
) as dag:

    # Task to check for new files in the data folder
    check_files_task = ShortCircuitOperator(
        task_id='check_for_new_data',
        python_callable=check_files,
        op_args=[data_folder],
    )

    # Task to upload new files to FastAPI
    upload_files_task = PythonOperator(
        task_id='upload_files',
        python_callable=upload_files,
    )

    # Set task dependencies
    check_files_task >> upload_files_task