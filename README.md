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

-- image link --

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
Finally, access the postgres database created by the container using Dbeaver or PGAdmin (or any tool you like for database management). Select your database and execute the SQL query below:
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

Then, you can use the code from the generate_files_final.ipynb file (available here: https://github.com/julienguyet/airflow-tutorial/blob/main/notebooks/generate_files_final.ipynb). 
This code creates data partitions including some issues in it (so we fake real life problems such as missing data in a ML pipeline). 
Do not forget to update the paths based on your own set up. 
Please note that the data used was from the Walmart Sales Prediction competition on Kaggle. You can download the it here: https://www.kaggle.com/datasets/aslanahmedov/walmart-sales-forecast

If you would like to work with your own dataset, please note you will need to update the Great Expectations rules defined in the Airflow Dag (line 34 and 35).

## 5. Execute the DAG
In your terminal, at the level of the airlfow folder, run the below:

```
astro dev start
```

You should see something like this:

<img width="650" alt="astro_start_up" src="https://github.com/julienguyet/dsp-project-JPS/assets/55974674/47ebb979-e306-4636-9c88-e1c1df8fe418">


This will start Airflow and a new tab will open in your browser. Connect by using the default credentials (admin / admin). 
You should see the dag in the list on the first page:

<img width="1511" alt="airflow_home_page" src="https://github.com/julienguyet/dsp-project-JPS/assets/55974674/6d163054-7f8e-404a-a8ba-295f90a64ded">


Click on the left button to activate it and voilà: You now have a dag running every one minute to check the quality of your data!
You can also get more details by clicking on the DAG and go to its graph tab:

<img width="1196" alt="airflow_workflow" src="https://github.com/julienguyet/dsp-project-JPS/assets/55974674/77ef1337-8de9-439a-8864-9a7578f2dcd5">

