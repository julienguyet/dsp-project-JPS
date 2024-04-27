from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models import XCom
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

db_params = {
    "host": "localhost",
    "port": 5433,
    "database": "public",
    "user": "postgres",
    "password": "1234"
}

teams_webhook = "https://epitafr.webhook.office.com/webhookb2/20776877-17e6-405b-bf9f-f0810f814f2a@3534b3d7-316c-4bc9-9ede-605c860f49d2/IncomingWebhook/1a33c6e51db14cb5861ae1833b3e578b/f5fc93d8-16f4-4ce7-950b-5f1d0d1c64dc"
report_directory = "../../gx/uncommitted/data_docs/local_site/validations/default/__none__"

@dag(
    start_date = datetime(2024, 3, 27),
    schedule = "*/1 * * * *",
    tags = ["DSP"],
    catchup = False
)

def validate_data_dag():

    @task
    def read_data() -> str:
        raw_data_directory = '/Users/julien/Documents/EPITA/S2/DSP/dsp-project-JPS/airflow/dags/corrupted_data'
        random_file = random.choice(os.listdir(raw_data_directory))
        file_path = os.path.join(raw_data_directory, random_file)
        return file_path

    @task
    def validate_data(file_path: str) -> dict:
    
        # Part I - Check Rules
        context = gx.get_context()
        validator = context.sources.pandas_default.read_csv(file_path)

        result_format: dict = {
        "result_format": "COMPLETE",
        "unexpected_index_column_names": ["Store","Dept","Date","Temperature","Fuel_Price",
                                        "MarkDown1","MarkDown2","MarkDown3","MarkDown4","MarkDown5",
                                        "CPI","Unemployment","IsHoliday","Type","Size"],}
        
        validator.expect_table_columns_to_match_ordered_list(column_list=["Store","Dept","Date","Temperature",
                                                                                                    "Fuel_Price","MarkDown1","MarkDown2",
                                                                                                    "MarkDown3","MarkDown4","MarkDown5",
                                                                                                    "CPI","Unemployment","IsHoliday","Type","Size"])
        
        to_not_be_null = ["Store","Dept","Date","Temperature","Fuel_Price","CPI","Unemployment","IsHoliday","Type","Size"]
        
        for col in to_not_be_null:
            validator.expect_column_values_to_not_be_null(column=col)
        
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
        checkpoint_result = checkpoint.run(result_format=result_format)
        

        # Part II - Statistics
        stats_key = list(checkpoint_result.get_statistics()['validation_statistics'])[0]
        result_json = checkpoint_result.get("run_results")
        statistics = checkpoint_result.get_statistics()['validation_statistics'][stats_key]
        report_link = result_json.get('run_results', {}).get(list(result_json.get('run_results', {}))[0], {}).get('actions_results', {}).get('update_data_docs', {}).get('local_site', None)
        encoded_report_link = urllib.parse.quote(report_link, safe=':/')

        total_expectations = statistics["evaluated_expectations"]
        successful_expectations = statistics["successful_expectations"]
        failed_expectations = statistics["unsuccessful_expectations"]
        percentage = statistics["success_percent"]

        # Part III - Errors retrieval
        run_results = checkpoint_result.get("run_results", {})
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
        
        # Part IV - Retrieve bad rows
        result_json = checkpoint_result.get("run_results")
        key_stats = list(checkpoint_result.get_statistics()['validation_statistics'])[0]
        statistics = checkpoint_result.get_statistics()['validation_statistics'][key_stats]
        succcess_ratio = statistics["success_percent"]
        result_dict_key = list(result_json.keys())[0]
        validation_result_info = result_json[result_dict_key]['validation_result']

        flag = False

        if validation_result_info['results'][0]["success"] == False and validation_result_info['results'][0]["expectation_config"]["expectation_type"] == "expect_table_columns_to_match_ordered_list":
            flag = True
        else:
            flag = False
        
        rows = []
        column_names = []

        for expectation_identifier, expectation_result in run_results.items():
            validation_result = expectation_result.get("validation_result", {})
            success = validation_result.get("success", False)
            results = validation_result.get("results", [])

        for result in results:
            result_expectation = result.get("expectation_config")
            if result_expectation.get("expectation_type") != "expect_column_values_to_be_of_type":
                result_info = result.get("result", {})
                unexpected_index_query = result_info.get("unexpected_index_query")
                if unexpected_index_query is not None and unexpected_index_query != "None":
                    print(unexpected_index_query)
                    rows.append(unexpected_index_query)
        for item in results:
            if item.get("success") == False and result_expectation.get("expectation_type") == "expect_column_values_to_be_of_type":
                if 'kwargs' in item['expectation_config']:
                    kwargs = item['expectation_config']['kwargs']
                    if 'column' in kwargs:
                        column_names.append(kwargs['column'])
        

        return total_expectations, successful_expectations, failed_expectations, percentage, encoded_report_link, expectation_data, succcess_ratio, flag, rows, column_names

    @task
    def send_alerts(total_expectations, successful_expectations, failed_expectations, percentage, encoded_report_link, teams_webhook):

        status = ""

        if percentage < 20:
            status = "LOW"
        if 20 < percentage < 50:
            status = "MEDIUM"
        if 50 < percentage < 80:
            status = "MAJOR"
        else:
            status = "CRITIC"
        
        alert = connectorcard(teams_webhook)
        alert.title(f"{status} ALERT")
        alert.text(f"{successful_expectations} rules succeeded, and {failed_expectations} rules failed out of {total_expectations}. Success ratio: {percentage}. To open the report in terminal, from dag folder run: `cd {encoded_report_link} && open *.html `")
        alert.send()
        
        print("Alert sent successfully.")
    
    @task
    def save_data_errors(db_params, expectation_data):

        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
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
    def save_file(success_ratio, flag, rows, column_names, file_path) -> None:

        numeric_columns = ['Store', 'Dept','Temperature', 'Fuel_Price','MarkDown1',
                    'MarkDown2', 'MarkDown3', 'MarkDown4', 'MarkDown5', 'CPI','Unemployment' ]
        type_valid_values = ["A", "B", "C"]
        holidays_valid_values = ["True", "False"]

        df = pd.read_csv(file_path)
        ct = datetime.now()
        ts = str(ct.timestamp())
        good_data_directory = '../airflow/dags/good_data'
        bad_data_directory = '../airflow/dags/bad_data'
        file_path_good_data = os.path.join(good_data_directory, f'good_data_{ts}.csv')
        file_path_bad_data = os.path.join(bad_data_directory, f'bad_data_{ts}.csv')

        if flag == True:
            shutil.move(file_path, os.path.join(bad_data_directory, os.path.basename(file_path)))
            print(f"Columns are in wrong order vs expectations, file store to bad data directory under name {os.path.join(bad_data_directory, os.path.basename(file_path))}")
        else:
            numbers_only = []
            for element in rows:
                numbers = re.findall(r'\d+', element)
                numbers_only.append(numbers)

            flattened_numbers = [number for sublist in numbers_only for number in sublist]
            flattened_numbers = [int(number) for number in flattened_numbers]

            rows_to_drop = np.unique(flattened_numbers)

            print(f"Len of DF: {len(df)}")
            print(f"Len of rows_to_keep: {len(rows_to_drop)}")

            rows_to_keep = []
            for i in range(len(df)):
                if i not in rows_to_drop:
                    rows_to_keep.append(i)
            
            print(f"rows to drop: {rows_to_drop}")
            print(f"length of rows to drop: {len(rows_to_drop)}")
            
            if success_ratio == 0.0:
                shutil.move(file_path, os.path.join(good_data_directory, os.path.basename(file_path)))
                print("file moved to good_data_directory")

            elif success_ratio >= 50:
                good_data = df.filter(items=rows_to_keep, axis=0)
                indices = []

                for idx, row in good_data.iterrows():
                    try:
                        pd.to_numeric(row[numeric_columns], errors='raise')
                        indices.append(idx)
                    except ValueError:
                        pass

                good_df = good_data.loc[indices]
                good_df[numeric_columns] = good_df[numeric_columns].astype(float)
                good_df = good_df[(good_df["Size"] >= 0)]
                good_df = good_df[(good_df["Fuel_Price"] >= 0)]
                good_df = good_df[(good_df["Unemployment"] >= 0)]
                good_df = good_df[good_df['Type'].isin(type_valid_values)]
                good_df = good_df[good_df['IsHoliday'].isin(holidays_valid_values)]

                bad_data = df.filter(items=rows_to_drop, axis=0)
                good_df.to_csv(file_path_good_data) 
                bad_data.to_csv(file_path_bad_data)
                os.remove(file_path)
                print("removed bad data from file")
            else:
                shutil.move(file_path, os.path.join(bad_data_directory, os.path.basename(file_path)))
                print("file corrupted ratio is too high, we drop it")

    # Task relationships
    file_path = read_data()
    total_expectations, successful_expectations, failed_expectations, percentage, encoded_report_link, expectation_data, success_ratio, flag, rows, column_names = validate_data(file_path, report_directory)
    send_alerts(total_expectations, successful_expectations, failed_expectations, percentage, encoded_report_link, teams_webhook)
    save_data_errors(db_params, expectation_data)
    save_file(success_ratio, flag, rows, column_names, file_path)

validate_data_dag()