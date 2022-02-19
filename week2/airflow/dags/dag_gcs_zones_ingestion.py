import os

from airflow import DAG
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from helpers import csv_to_parquet, upload_to_gcs
from airflow.utils.dates import days_ago

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow') 
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# Schedule ingestion every 2nd day of the month.
local_workflow = DAG(
    "Zones-DAG",
    start_date=days_ago(1),
    catchup=True,
    max_active_runs=2
)

URL_BASE = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv' 
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/zones.csv'

with local_workflow:

    # download 
    download_csv = BashOperator(
        task_id="zones-wget-csv",
        bash_command=f"wget {URL_BASE} -O {OUTPUT_FILE_TEMPLATE}"
    )

    # Parquet
    to_parquet = PythonOperator(
        task_id="zones-parquet",
        python_callable=csv_to_parquet,
        op_kwargs={
            "original_path": OUTPUT_FILE_TEMPLATE,
            "delete_csv": True
        }
    )

    # Upload it to GCS
    local_to_gcs_task = PythonOperator(
        task_id="zones-gcs-task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{OUTPUT_FILE_TEMPLATE.split('/')[-1].replace('.csv','.parquet')}",
            "local_file": f"{OUTPUT_FILE_TEMPLATE.replace('.csv','.parquet')}",
        },
    )

download_csv >> to_parquet >> local_to_gcs_task