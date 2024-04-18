from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import random
import great_expectations as ge
import shutil
import pandas as pd
from collections import namedtuple

DataValidationResults = namedtuple(
    "DataValidationResults", ["df", "corrupted_ratio", "rows", "file_path"]
)

@dag(
    start_date = datetime(2024, 3, 27),
    schedule = "*/1 * * * *",
    tags = ["DSP"],
    catchup = False
)

def data_validation_dag():

    @task
    def read_data() -> (pd.DataFrame, str):

        raw_data_directory = '/usr/local/airflow/dags/corrupted_data'
        random_file = random.choice(os.listdir(raw_data_directory))
        file_path = os.path.join(raw_data_directory, random_file)
        df = ge.read_csv(file_path)

        return df, file_path

    @task
    def validate_data(df: pd.DataFrame, file_path: str) -> dict :

        rows = []

        for i in range(0, len(df)):
            temp_df = df.iloc[[i]]
            date_quality = dict(temp_df.expect_column_values_to_not_be_null(column="Date"))
            cpi_quality = dict(temp_df.expect_column_values_to_not_be_null(column="CPI"))

            if not date_quality["success"] and i not in rows:
                rows.append(i)

            elif not cpi_quality["success"] and i not in rows:
                rows.append(i)

        corrupted_ratio = len(rows) / len(df)

        return {
            "df_json": df.to_json(),
            "corrupted_ratio": corrupted_ratio,
            "rows": rows,
            "file_path": file_path,
        }

    @task
    def send_alerts():
        return
    
    @task
    def save_file(data: dict) -> None:
        df_json, corrupted_ratio, rows, file_path = data.values()
        df = pd.read_json(df_json)
        
        ct = datetime.now()
        ts = str(ct.timestamp())
        good_data_directory = '/usr/local/airflow/dags/good_data'
        bad_data_directory = '/usr/local/airflow/dags/bad_data'
        file_path_good_data = os.path.join(good_data_directory, f'good_data_{ts}.csv')
        file_path_bad_data = os.path.join(bad_data_directory, f'bad_data_{ts}.csv')

        if corrupted_ratio == 0:
            shutil.move(file_path, os.path.join(good_data_directory, os.path.basename(file_path)))
        elif corrupted_ratio <= 0.50:
            good_data = df.drop(index=rows)
            bad_data = df.iloc[rows]
            good_data.to_csv(file_path_good_data) 
            bad_data.to_csv(file_path_bad_data)
            os.remove(file_path)
        else:
            shutil.move(file_path, os.path.join(bad_data_directory, os.path.basename(file_path)))

    @task
    def save_data_errors(data: dict) -> None:

        return
    

    # Task relationships

data_validation_dag()