from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models import XCom
from datetime import datetime
import os
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

DataValidationResults = namedtuple(
    "DataValidationResults", ["df", "corrupted_ratio", "rows", "file_path"]
)

db_params = {
    "host": "localhost",
    "port": 5433,
    "database": "public",
    "user": "postgres",
    "password": "1234"
}

teams_webhook = "https://epitafr.webhook.office.com/webhookb2/20776877-17e6-405b-bf9f-f0810f814f2a@3534b3d7-316c-4bc9-9ede-605c860f49d2/IncomingWebhook/1a33c6e51db14cb5861ae1833b3e578b/f5fc93d8-16f4-4ce7-950b-5f1d0d1c64dc"
report_directory = "/usr/local/airflow/dags/reports"

@dag(
    start_date = datetime(2024, 3, 27),
    schedule = "*/1 * * * *",
    tags = ["DSP"],
    catchup = False
)

def validate_data_dag():

    @task
    def read_data() -> str:
        raw_data_directory = '/usr/local/airflow/dags/corrupted_data'
        random_file = random.choice(os.listdir(raw_data_directory))
        file_path = os.path.join(raw_data_directory, random_file)
        return file_path

    @task
    def validate_data(file_path: str) -> dict:

        context = gx.get_context()
        validator = context.sources.pandas_default.read_csv(file_path)
        df = pd.read_csv(file_path)
        validator.expect_table_columns_to_match_ordered_list(column_list=["Store","Dept","Date","Temperature",
                                                                                                    "Fuel_Price","MarkDown1","MarkDown2",
                                                                                                    "MarkDown3","MarkDown4","MarkDown5",
                                                                                                    "CPI","Unemployment","IsHoliday","Type","Size"])
        # Execute all the validation rules
        for column in df.columns:
            validator.expect_column_values_to_not_be_null(column=column)
        
        validator.expect_column_values_to_be_of_type(column="Store", type_='int64')
        validator.expect_column_values_to_be_of_type(column="Dept", type_='int64')
        validator.expect_column_values_to_be_of_type(column="Date", type_='object')
        validator.expect_column_values_to_be_of_type(column="Temperature", type_='float64')
        validator.expect_column_values_to_be_of_type(column="Fuel_Price", type_='float64')
        validator.expect_column_values_to_be_of_type(column="MarkDown1", type_='float64')
        validator.expect_column_values_to_be_of_type(column="MarkDown2", type_='float64')
        validator.expect_column_values_to_be_of_type(column="MarkDown3", type_='float64')
        validator.expect_column_values_to_be_of_type(column="MarkDown4", type_='float64')
        validator.expect_column_values_to_be_of_type(column="MarkDown5", type_='float64')
        validator.expect_column_values_to_be_of_type(column="CPI", type_='float64')
        validator.expect_column_values_to_be_of_type(column="Unemployment", type_='float64')
        validator.expect_column_values_to_be_of_type(column="IsHoliday", type_='bool')
        validator.expect_column_values_to_be_of_type(column="Type", type_='object')
        validator.expect_column_values_to_be_of_type(column="Size", type_='int64')

        validator.save_expectation_suite(discard_failed_expectations=False)

        checkpoint = context.add_or_update_checkpoint(
            name="dsp_checkpoint",
            validator=validator
        )
        checkpoint_result = checkpoint.run()

        return checkpoint_result

    @task
    def save_data_errors(db_params, result_json):

        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        run_results = result_json.get("run_results", {})
        
        expectation_data = {}

        for expectation_identifier, expectation_result in run_results.items():
            # Accessing the expectation result dictionary
            validation_result = expectation_result.get("validation_result", {})
            success = validation_result.get("success", False)
            results = validation_result.get("results", [])

            # Iterating over the results for each expectation
            for result in results:
                expectation_config = result.get("expectation_config", {})
                expectation_type = expectation_config.get("expectation_type", "")
                kwargs = expectation_config.get("kwargs", {})
                column = kwargs.get("column", "all_columns" if not kwargs.get("column") else kwargs.get("column"))
                
                result_info = result.get("result", {})
                element_count = result_info.get("element_count", 0)
                unexpected_count = result_info.get("unexpected_count", 0)
                unexpected_percent = result_info.get("unexpected_percent", 0.0)

                # Store the extracted information in the dictionary
                if column not in expectation_data:
                    expectation_data[column] = []

                # Append the information for the current expectation to the list
                expectation_data[column].append({
                    "success": success,
                    "expectation_type": expectation_type,
                    "element_count": element_count,
                    "unexpected_count": unexpected_count,
                    "unexpected_percent": unexpected_percent
                })
        
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=db_params['database'],
            user=db_params['user'],
            password=db_params['password'],
            host=db_params['host'],
            port=db_params['port']
        )

        # Create a cursor object using the cursor() method
        cursor = conn.cursor()

        # Loop through the expectation data and insert into the database
        for column, data in expectation_data.items():
            for entry in data:
                # Extract relevant information
                rule = entry['expectation_type']
                rows = entry['element_count']
                missing_values = entry['unexpected_count']
                percentage = entry['unexpected_percent']
                # Calculate criticality based on percentage, adjust as needed
                criticality = 0
                if percentage == 0:
                        criticality = 1
                elif percentage <= 0.25:
                    criticality = 2
                elif percentage <= 0.5:
                    criticality = 3
                elif percentage <= 0.75:
                    criticality = 4
                else:
                    criticality = 5

                # Prepare SQL query to insert data
                insert_query = sql.SQL("INSERT INTO data_quality_errors (date, rule, rows, missing_values, percentage, criticality) VALUES (%s, %s, %s, %s, %s, %s)")

                # Execute the SQL query
                cursor.execute(insert_query, (current_datetime, rule, rows, missing_values, percentage, criticality))

        # Commit changes
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        print("Data errors saved successfully.")

    @task
    def send_alerts(checkpoint_result, result_json, teams_webhook):

        key = list(checkpoint_result.get_statistics()['validation_statistics'])[0]
        report_link = result_json.get('run_results', {}).get(list(result_json.get('run_results', {}))[0], {}).get('actions_results', {}).get('update_data_docs', {}).get('local_site', None)
        encoded_report_link = urllib.parse.quote(report_link, safe=':/')
        statistics = checkpoint_result.get_statistics()['validation_statistics'][key]

        total_expectations = statistics["evaluated_expectations"]
        successful_expectations = statistics["successful_expectations"]
        failed_expecations = statistics["unsuccessful_expectations"]
        percentage = statistics["success_percent"]

        status = ""

        if percentage < 20:
            status = "LOW"
        if 20 < percentage < 50:
            status = "MEDIUM"
        if 50 < percentage < 80:
            status = "MAJOR"
        else:
            stayus = "CRITIC"

        alert = connectorcard(teams_webhook)
        alert.title(f"{status} ALERT")
        alert.text(f"{successful_expectations} rules succeeded, and {failed_expecations} rules failed out of {total_expectations}. Success ratio: {percentage}. To open the report in terminal run: `cd {encoded_report_link[7:149]} && open *.html `")
        alert.send()

        print("Alert sent successfully.")


    @task
    def save_file(checkpoint_result: dict, file_path) -> None:
        
        run_results = result_json.get("run_results", {})
        key = list(checkpoint_result.get_statistics()['validation_statistics'])[0]
        statistics = checkpoint_result.get_statistics()['validation_statistics'][key]
        corrupted_ratio = statistics["success_percent"]

        df = pd.read_csv(file_path)
        ct = datetime.now()
        ts = str(ct.timestamp())
        good_data_directory = '../airflow/dags/good_data'
        bad_data_directory = '../airflow/dags/bad_data'
        file_path_good_data = os.path.join(good_data_directory, f'good_data_{ts}.csv')
        file_path_bad_data = os.path.join(bad_data_directory, f'bad_data_{ts}.csv')

        indexes = []

        for expectation_identifier, expectation_result in run_results.items():
            validation_result = expectation_result.get("validation_result", {})
            success = validation_result.get("success", False)
            results = validation_result.get("results", [])
            for result in results:
                result_info = result.get("result", {})
                rows = result_info.get("partial_unexpected_index_list")
                if rows != None:
                    for row in rows:
                        if row not in indexes:
                            indexes.append(row)

        print(corrupted_ratio)
        print(file_path)
        
        if corrupted_ratio == 0.0:
            shutil.move(file_path, os.path.join(good_data_directory, os.path.basename(file_path)))
            print("file moved to good_data_directory")
        elif corrupted_ratio <= 50:
            good_data = df.drop(index=indexes)
            bad_data = df.iloc[indexes]
            good_data.to_csv(file_path_good_data) 
            bad_data.to_csv(file_path_bad_data)
            os.remove(file_path)
            print("removed bad data from file")
        else:
            shutil.move(file_path, os.path.join(bad_data_directory, os.path.basename(file_path)))
            print("file corrupted ratio is too high, we drop it")

    # Task relationships
    file_path = read_data()
    checkpoint_result, result_json = validate_data(file_path)
    send_alerts(checkpoint_result, result_json, teams_webhook)
    save_data_errors(db_params, result_json)
    save_file(checkpoint_result, result_json, file_path)

validate_data_dag()