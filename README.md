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

## 2. Install Astro
Now, we will install Astro which is an environment manager for Airflow and Docker. It will make our life easier by setting up the environment and managing the connection between Docker and Airflow.
Go to below link and follow instructions based on your OS. Once installation is finished move to step 3. 
https://docs.astronomer.io/astro/cli/install-cli

## 3. Set up Astro
If you wish to create your own airflow folder, follow the steps below. If you cloned the repository and would like to use the provided airflow folder, you can jump to section 4.

Move to your directory where you created your repo and at the root, in terminal, do in order:

```
mkdir airflow
```

```
cd airflow
```

Once you are inside the airflow directory execute the below command. It will initialize your Astro environment:
```
astro dev init
```

You should now see new files and folders added to the airflow directory. Please note that by default astro will add the airflow folder to the .gitignore file.

## 4. Create the Airflow DAG
In your aiflow directory, go to the 'dags' folder and save here the 'data_validation_dag.py' file available at: https://github.com/julienguyet/airflow-tutorial/blob/main/airflow/dags/data_validation_dag.py

After that, in the dag folder execute the below commands:
```
mkdir raw_data
```
```
mkdir bad_data
```
```
mkdir good_data
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

