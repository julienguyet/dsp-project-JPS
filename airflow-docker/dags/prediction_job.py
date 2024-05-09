from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
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
fastapi_url = "http://localhost:8000"

def check_files(data_folder):
    record_file_path = os.path.join(data_folder, "processed_files.txt")
    if not os.path.exists(record_file_path):
        open(record_file_path, 'w').close()

    with open(record_file_path, 'r') as record_file:
        processed_files = record_file.read().splitlines()

    files_in_folder = os.listdir(data_folder)

    new_files = [file for file in files_in_folder if file not in processed_files]

    if new_files:
        with open(record_file_path, 'a') as record_file:
            record_file.write("\n".join(new_files))
            record_file.write("\n")
        return True
    else:
        return False

def predict_new_files(**kwargs):
    task_instance = kwargs['task_instance']
    new_files = task_instance.xcom_pull(task_ids='check_for_new_data')
    if new_files:
        print("Processing new files:", new_files)
        # Forward new files to FastAPI
        response = requests.post(fastapi_url, json={"new_files": new_files})
        if response.status_code == 200:
            print("New files forwarded to FastAPI successfully")
        else:
            print("Failed to forward new files to FastAPI:", response.text)
    else:
        print("No new files to process")

with DAG(
    'prediction_job',
    default_args=default_args,
    description='A DAG to check for new files in the data folder and make predictions',
    schedule_interval="*/2 * * * *",
    catchup=False,
) as dag:

    check_files_task = ShortCircuitOperator(
        task_id='check_for_new_data',
        python_callable=check_files,
        op_args=[data_folder],
    )

    prediction_task = PythonOperator(
        task_id='make_predictions',
        python_callable=predict_new_files,
    )

    check_files_task >> prediction_task

