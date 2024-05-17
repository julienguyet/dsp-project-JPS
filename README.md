# Environment Setup

In order to replicate the environment, clone the repository, activate you virtual environment and then run:

```
pip install -r requirements.txt
```

Now you can move to the next steps

# Airflow Tutorial
A Git Repository to demonstrate a data validation job using Airflow: we will run an airflow job every one minute to check the data quality of a file and remove bad data from our ingestion pipeline. Then, another dag will make API calls to run ML predictions on the data we cleaned in the previous step.
If you would like to replicate this environment on your own machine, you can follow the steps below. 

## 1. Install Docker
The first thing to do is to install docker (if already installed, you can move to step 2).
Go to this [link](https://www.docker.com/get-started/) and follow the instructions based on your OS.

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
# grant all previlages to the user (change myuser by your user name in postgres)
GRANT SELECT ON TABLE features TO myuser;
GRANT INSERT, UPDATE, DELETE ON TABLE features TO myuser;
```

## 4. Create the files

Go to the notebook folder in your repository and open the generate_files_final.ipynb file (available [here](https://github.com/julienguyet/airflow-tutorial/blob/main/notebooks/generate_files_final.ipynb)). 
This code creates data partitions including some issues in it (so we fake real life problems such as missing data in a ML pipeline). 
Do not forget to update the paths based on your own set up. 
Please note that the data used was from the Walmart Sales Prediction competition on Kaggle. You can download the it on [Kaggle](https://www.kaggle.com/datasets/aslanahmedov/walmart-sales-forecast).

If you would like to work with your own dataset, please note you will need to update the Great Expectations rules defined in the Airflow Dag.

## 5. Create database connections

To be able to save data to your database when Airflow runs, you need to update the database URL. First, in the fastapi_app main.py replace DATABASE_URL with yours.
Then, in the docker app, run your container and in your browser go to http://localhost:8080/home . Log in using the default credentials (admin/admin). In the admin tab go to "connections" section:

<img width="793" alt="Screenshot 2024-05-14 at 16 19 16" src="https://github.com/julienguyet/dsp-project-JPS/assets/55974674/ad664c5c-b412-474c-b9a0-d4d26330eff6">

Click on "add new record" and select Postgres as connection type. Then fill in the fields with your corresponding credentials. For example, ours looks like this:

<img width="484" alt="Screenshot 2024-05-14 at 16 21 44" src="https://github.com/julienguyet/dsp-project-JPS/assets/55974674/92232d22-3ad9-4441-8494-dd2b17bb63d7">

## 6. Create http connection

While you are in the connections section of Airflow, create a new one and this time select type as HTTP. Fill in the following fields as below and leave the rest empty:
- Connection Id: http_conn_id
- Host: host.docker.internal
- Port: 8000

## 7. Create Teams Webhook

If you wish to receive alerts to your Microsoft Teams, you can follow instructions at this [link](https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook?tabs=newteams%2Cdotnet). Then update the TEAMS_WEBHOOK variable in the __init__.py file in the airflow-docker/dags_data_validation folder. 

## 8. Run the Airflow Dags

In your terminal, at the root of the directory, execute below commands:
```
uvicorn fastapi_app.main:app --reload
```
```
streamlit run streamlit_app/user_interface.py --server.enableXsrfProtection=false
```

The first one will start the Fast API and the second open the streamlit app where you can manually upload files or insert data to run the prediction job. 

Finally, go back to the Airflow home and active the two dags like below:

<img width="291" alt="Screenshot 2024-05-14 at 16 44 04" src="https://github.com/julienguyet/dsp-project-JPS/assets/55974674/6ab3dad9-60cd-4eef-a92f-bdbd8d2a60ba">

## 9. Dashboards

Once the dags are running, data is saved instantly in the database. If you wish to monitor the errors and performance of the prediction model you can use the dashboard templates saved in the grafana folder. Upload them in [Grafana](https://grafana.com/) and you will obtain those:

Data Quality Dashboard:

<img width="1301" alt="Screenshot 2024-05-17 at 11 22 40" src="https://github.com/julienguyet/dsp-project-JPS/assets/55974674/ea874734-0373-49be-9821-241f2c4d6b59">

Model Performance Dashboard:

<img width="595" alt="Screenshot 2024-05-17 at 22 57 39" src="https://github.com/julienguyet/dsp-project-JPS/assets/55974674/fd4c6b01-6fdc-4e0a-b92f-680b3ce238ef">
