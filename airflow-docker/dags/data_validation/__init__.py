import os
#DAG_DIRECTORY = '/usr/local/airflow-docker/dags'
DAG_DIRECTORY = '/opt/airflow/dags'
RAW_DATA_DIRECTORY = os.path.join(DAG_DIRECTORY, 'corrupted_data')
REPORT_DIRECTORY= "../../gx/uncommitted/data_docs/local_site/validations/default/__none__"
TEAMS_WEBHOOK = "https://epitafr.webhook.office.com/webhookb2/20776877-17e6-405b-bf9f-f0810f814f2a@3534b3d7-316c-4bc9-9ede-605c860f49d2/IncomingWebhook/1a33c6e51db14cb5861ae1833b3e578b/f5fc93d8-16f4-4ce7-950b-5f1d0d1c64dc"

DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "postgres",
    "password": "postgres"
}
GOOD_DATA_DIRECTORY = os.path.join(DAG_DIRECTORY, 'good_data')
BAD_DATA_DIRECTORY = os.path.join(DAG_DIRECTORY, 'bad_data')