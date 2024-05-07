from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from datetime import datetime
import os
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
        task_id='process_new_files',
        python_callable=predict_new_files,
    )

    check_files_task >> prediction_task
