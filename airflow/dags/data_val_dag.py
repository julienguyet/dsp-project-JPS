from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import random
import great_expectations as ge
import shutil
import pandas as pd
from collections import namedtuple
import psycopg2
from psycopg2 import sql
from jinja2 import Environment, FileSystemLoader
from pymsteams import connectorcard

DataValidationResults = namedtuple(
    "DataValidationResults", ["df", "corrupted_ratio", "rows", "file_path"]
)

db_params = {
    "host": "localhost",
    "database": "public",
    "user": "postgres",
    "password": "ma_databas€"
    }

teams_webhook = "https://epitafr.webhook.office.com/webhookb2/20776877-17e6-405b-bf9f-f0810f814f2a@3534b3d7-316c-4bc9-9ede-605c860f49d2/IncomingWebhook/1a33c6e51db14cb5861ae1833b3e578b/f5fc93d8-16f4-4ce7-950b-5f1d0d1c64dc"
report_directory = "/usr/local/airflow/dags/reports"

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
    def validate_data(df: pd.DataFrame, file_path: str) -> dict:
        validation_results = []

        for i in range(len(df)):
            row_results = {"row_index": i, "failed_rules": [], "passed_rules": []}
            temp_df = df.iloc[[i]]

            # Execute all the validation rules
            date_quality = dict(temp_df.expect_column_values_to_not_be_null(column="Date"))
            cpi_quality = dict(temp_df.expect_column_values_to_not_be_null(column="CPI"))
            column_order = dict(temp_df.expect_table_columns_to_match_ordered_list(column_list=["Store","Dept","Date","Temperature",
                                                                                                "Fuel_Price","MarkDown1","MarkDown2",
                                                                                                "MarkDown3","MarkDown4","MarkDown5",
                                                                                                "CPI","Unemployment","IsHoliday","Type","Size"]))
            store_type_quality = dict(temp_df.expect_column_values_to_be_of_type(column="Store", type_='int64'))
            dept_type_quality = dict(temp_df.expect_column_values_to_be_of_type(column="Dept", type_='int64'))
            date_type_quality = dict(temp_df.expect_column_values_to_be_of_type(column="Date", type_='object'))
            weekly_sales_type_quality = dict(temp_df.expect_column_values_to_be_of_type(column="Weekly_Sales", type_='float64'))
            temperature_type_quality = dict(temp_df.expect_column_values_to_be_of_type(column="Temperature", type_='float64'))
            fuel_price_type_quality = dict(temp_df.expect_column_values_to_be_of_type(column="Fuel_Price", type_='float64'))
            markdown1_type_quality = dict(temp_df.expect_column_values_to_be_of_type(column="MarkDown1", type_='float64'))
            markdown2_type_quality = dict(temp_df.expect_column_values_to_be_of_type(column="MarkDown2", type_='float64'))
            markdown3_type_quality = dict(temp_df.expect_column_values_to_be_of_type(column="MarkDown3", type_='float64'))
            markdown4_type_quality = dict(temp_df.expect_column_values_to_be_of_type(column="MarkDown4", type_='float64'))
            markdown5_type_quality = dict(temp_df.expect_column_values_to_be_of_type(column="MarkDown5", type_='float64'))
            cpi_type_quality = dict(temp_df.expect_column_values_to_be_of_type(column="CPI", type_='float64'))
            unemployment_type_quality = dict(temp_df.expect_column_values_to_be_of_type(column="Unemployment", type_='float64'))
            isholiday_type_quality = dict(temp_df.expect_column_values_to_be_of_type(column="IsHoliday", type_='bool'))
            type_type_quality = dict(temp_df.expect_column_values_to_be_of_type(column="Type", type_='object'))
            size_type_quality = dict(temp_df.expect_column_values_to_be_of_type(column="Size", type_='int64'))

            # Check which rules failed and which ones passed
            if not date_quality["success"]:
                row_results["failed_rules"].append("date_quality")
            else:
                row_results["passed_rules"].append("date_quality")

            if not cpi_quality["success"]:
                row_results["failed_rules"].append("cpi_quality")
            else:
                row_results["passed_rules"].append("cpi_quality")

            if not column_order["success"]:
                row_results["failed_rules"].append("column_order")
            else:
                row_results["passed_rules"].append("column_order")

            if not store_type_quality["success"]:
                row_results["failed_rules"].append("store_type_quality")
            else:
                row_results["passed_rules"].append("store_type_quality")

            if not dept_type_quality["success"]:
                row_results["failed_rules"].append("dept_type_quality")
            else:
                row_results["passed_rules"].append("dept_type_quality")

            if not date_type_quality["success"]:
                row_results["failed_rules"].append("date_type_quality")
            else:
                row_results["passed_rules"].append("date_type_quality")

            if not weekly_sales_type_quality["success"]:
                row_results["failed_rules"].append("weekly_sales_type_quality")
            else:
                row_results["passed_rules"].append("weekly_sales_type_quality")

            if not temperature_type_quality["success"]:
                row_results["failed_rules"].append("temperature_type_quality")
            else:
                row_results["passed_rules"].append("temperature_type_quality")

            if not fuel_price_type_quality["success"]:
                row_results["failed_rules"].append("fuel_price_type_quality")
            else:
                row_results["passed_rules"].append("fuel_price_type_quality")

            if not markdown1_type_quality["success"]:
                row_results["failed_rules"].append("markdown1_type_quality")
            else:
                row_results["passed_rules"].append("markdown1_type_quality")

            if not markdown2_type_quality["success"]:
                row_results["failed_rules"].append("markdown2_type_quality")
            else:
                row_results["passed_rules"].append("markdown2_type_quality")

            if not markdown3_type_quality["success"]:
                row_results["failed_rules"].append("markdown3_type_quality")
            else:
                row_results["passed_rules"].append("markdown3_type_quality")

            if not markdown4_type_quality["success"]:
                row_results["failed_rules"].append("markdown4_type_quality")
            else:
                row_results["passed_rules"].append("markdown4_type_quality")

            if not markdown5_type_quality["success"]:
                row_results["failed_rules"].append("markdown5_type_quality")
            else:
                row_results["passed_rules"].append("markdown5_type_quality")

            if not cpi_type_quality["success"]:
                row_results["failed_rules"].append("cpi_type_quality")
            else:
                row_results["passed_rules"].append("cpi_type_quality")

            if not unemployment_type_quality["success"]:
                row_results["failed_rules"].append("unemployment_type_quality")
            else:
                row_results["passed_rules"].append("unemployment_type_quality")

            if not isholiday_type_quality["success"]:
                row_results["failed_rules"].append("isholiday_type_quality")
            else:
                row_results["passed_rules"].append("isholiday_type_quality")

            if not type_type_quality["success"]:
                row_results["failed_rules"].append("type_type_quality")
            else:
                row_results["passed_rules"].append("type_type_quality")

            if not size_type_quality["success"]:
                row_results["failed_rules"].append("size_type_quality")
            else:
                row_results["passed_rules"].append("size_type_quality")

            # Append the results for this row to the overall validation results
            validation_results.append(row_results)

        corrupted_ratio = len([result for result in validation_results if result["failed_rules"]]) / len(df)

        return {
            "validation_results": validation_results,
            "corrupted_ratio": corrupted_ratio,
            "file_path": file_path,
        }

    @task
    def save_data_errors(validation_results, db_params):
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()

            # Get current date and time
            current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            for result in validation_results:
                row_index = result["row_index"]
                failed_rules = result["failed_rules"]
                success_rules = result["passed_rules"]

                # Calculate relevant statistics
                rows = len(failed_rules) + len(success_rules)
                missing_values = len(failed_rules)
                percentage = missing_values / rows if rows > 0 else 0

                # Determine criticality based on percentage
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

                for rule in failed_rules:
                    cur.execute(sql.SQL("""
                        INSERT INTO data_quality_errors (date, rule, rows, missing_values, percentage, criticality)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """), (current_datetime, rule, rows, missing_values, percentage, criticality))
                    conn.commit()

            # Close the cursor and connection
            cur.close()
            conn.close()

            print("Data errors saved successfully.")

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL:", error)

    @task
    def send_alerts(validation_results, db_params, teams_webhook, report_directory):
        try:
            # Establish a connection to the PostgreSQL database
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()

            # Determine criticality of data problem
            num_failed_rules = sum(len(result["failed_rules"]) for result in validation_results)
            if num_failed_rules > 5:
                criticality = "High"
            elif num_failed_rules > 2:
                criticality = "Medium"
            else:
                criticality = "Low"

            # Generate validation report HTML content
            env = Environment(loader=FileSystemLoader('.'))
            template = env.get_template('validation_report_template.html')
            html_content = template.render(validation_results=validation_results)

            # Create report directory if it doesn't exist
            if not os.path.exists(report_directory):
                os.makedirs(report_directory)

            # Generate report file path
            report_date = datetime.now().strftime("%Y-%m-%d")
            report_file = os.path.join(report_directory, f'data_val_report_{report_date}.html')

            # Write HTML content to file
            with open(report_file, 'w') as f:
                f.write(html_content)

            # Send alert using Microsoft Teams
            alert = connectorcard(teams_webhook)
            alert.title(f"{num_failed_rules} data problem(s) detected with {criticality} criticality.")
            alert.text(f"Link to the report: file://{report_file}")
            alert.send()

            # Update data quality statistics in PostgreSQL database
            for result in validation_results:
                row_index = result["row_index"]
                failed_rules = result["failed_rules"]

                for rule in failed_rules:
                    cur.execute(sql.SQL("""
                        UPDATE data_quality_statistics
                        SET count = count + 1
                        WHERE rule = %s
                    """), (rule,))
                    conn.commit()

            print("Alert sent successfully.")

            # Close the cursor and connection
            cur.close()
            conn.close()

        except (Exception, psycopg2.Error) as error:
            print("Error:", error)
    
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

    

    # Task relationships
    df, file_path = read_data()
    results = validate_data(df, file_path)
    save_data_errors(results, db_params)
    send_alerts(results, db_params, teams_webhook, report_directory)
    save_file(results)

data_validation_dag()