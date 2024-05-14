# Environment Setup

In order to replicate the environment, clone the repository, activate you virtual environment and then run:

```
pip install -r requirements.txt
```

Now you can move to the next steps

# Airflow Tutorial
A Git Repository to demonstrate a simple data validation job using Airflow: we will run an airflow job every one minute to check the data quality of a file and remove bad data from our ingestion pipeline.
If you would like to replicate this environment on your own machine, you can follow the steps below. 

## 1. Install Docker
The first thing to do is to install docker (if already installed, you can move to step 2).
Go to https://www.docker.com/get-started/ and follow the instructions based on your OS.

## 2. Build the container
Now, go to the airflow-docker folder and open a terminal. Then run the below command to build the container.

```
docker compose up 
```

## 3. Run the container and set up environment
Open the docker app on your laptop and run the container you just created. You should see something like this:

<img width="983" alt="Screenshot 2024-05-14 at 16 05 22" src="https://github.com/julienguyet/dsp-project-JPS/assets/55974674/e69cd600-7def-4559-9a0f-ed857500c950">

In the aiflow-docker directory, go to the 'dags' folder and execute the below commands:
```
mkdir raw_data
```
```
mkdir bad_data
```
```
mkdir good_data
```
```
mkdir corrupted_data
```
Finally, access the postgres database created by the container using Dbeaver or PGAdmin (or any tool you like for database management). Select your database and execute the SQL queries below:

```
-- Create table for storing data quality errors
CREATE TABLE data_quality_errors (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP,
    rule VARCHAR(255),
    rows INT,
    missing_values INT,
    percentage FLOAT,
    criticality INT
);
```

```
CREATE TABLE features (
        id SERIAL PRIMARY KEY,
        "Store" FLOAT,
        "Dept" FLOAT,
        "Date" VARCHAR,
        "Weekly_Sales" FLOAT,
        "Temperature" FLOAT,
        "Fuel_Price" FLOAT,
        "MarkDown1" FLOAT,
        "MarkDown2" FLOAT,
        "MarkDown3" FLOAT,
        "MarkDown4" FLOAT,
        "MarkDown5" FLOAT,
        "CPI" FLOAT,
        "Unemployment" FLOAT,
        "IsHoliday" BOOLEAN,
        "Type" VARCHAR,
        "Size" FLOAT,
        "Sales" FLOAT,
        "pred_date" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
```

## 4. Create the files

Go to the notebook folder in your repository and open the generate_files_final.ipynb file (available here: https://github.com/julienguyet/airflow-tutorial/blob/main/notebooks/generate_files_final.ipynb). 
This code creates data partitions including some issues in it (so we fake real life problems such as missing data in a ML pipeline). 
Do not forget to update the paths based on your own set up. 
Please note that the data used was from the Walmart Sales Prediction competition on Kaggle. You can download the it here: https://www.kaggle.com/datasets/aslanahmedov/walmart-sales-forecast

If you would like to work with your own dataset, please note you will need to update the Great Expectations rules defined in the Airflow Dag.

