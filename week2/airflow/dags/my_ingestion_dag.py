import os

from airflow import DAG
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_files import ingest_callable


AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow') 

# Schedule ingestion every 2nd day of the month.
local_workflow = DAG(
    "MyIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1),
    catchup=True,
    max_active_runs=2
)

URL_BASE = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata' 
URL_TEMPLATE = URL_BASE + '_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

print(f'OS ENV ARE {PG_HOST}, {PG_PASSWORD}, {PG_USER}, {PG_PORT}, {PG_DATABASE}')

with local_workflow:

    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'wget {URL_TEMPLATE} -O {OUTPUT_FILE_TEMPLATE}',
    )
    

    ingest_task = PythonOperator(
        task_id='ingest',
        python_callable=ingest_callable,
        op_kwargs=dict(
            host=PG_HOST,
            user=PG_USER,
            password=PG_PASSWORD,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            csv_name=OUTPUT_FILE_TEMPLATE
        )
    )

    delete_task = BashOperator(
        task_id='delete',
        bash_command=f'rm {OUTPUT_FILE_TEMPLATE}',
    )



wget_task >> ingest_task >> delete_task