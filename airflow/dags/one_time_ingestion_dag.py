import os

import logging
import json

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.utils.dates import datetime

import pyarrow.csv as pv
import pyarrow.parquet as pq


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
S3_DESTINATION= 'raw/cycling-extras'
S3_BUCKET = os.environ.get("S3_BUCKET", "s3_no_bucket")
download_links= [
    {   
        'type': 'stations',
        'link': 'https://www.whatdotheyknow.com/request/664717/response/1572474/attach/3/Cycle%20hire%20docking%20stations.csv.txt',
        'output': 'stations.csv'
    },
    {
        'type': 'weather',
        'link': '--no-check-certificate "https://docs.google.com/uc?export=download&id=1Aa2mP5CwLele94GkJWqvpCmlm6GXeu8c"',
        'output': 'weather.json'
    }
]

# extract days value from the weather data
def dates_extractor():
    filepath= f"{path_to_local_home}/{download_links[1]['output']}"
    
    with open(filepath, 'r') as f:
        weather = json.load(f)

    days= weather['days']
    
    with open(filepath, 'w') as f:
        json.dump(days, f)



default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="one_time_ingestion_dag",
    description="""
        This dag ingests extra files for the cycling journey including: the docking stations and the weather data.
    """, 
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'stations', 'docking stations', 'london weather', 'london 2021'],
) as dag:

    for item in download_links:
        
        file_download = BashOperator(
            task_id=f"download_{item['type']}_task",
            bash_command=f"wget {item['link']} -O {path_to_local_home}/{item['output']}"
        )


        upload_to_s3 = LocalFilesystemToS3Operator(
            task_id=f"upload_{item['type']}_to_s3_task",
            filename=item['output'],
            dest_key=f"{S3_DESTINATION}/{item['output']}",
            dest_bucket=S3_BUCKET,
        )


        if item['type'] == 'weather':

            extract_dates = PythonOperator(
                task_id="extract_weather_dates_task",
                python_callable=dates_extractor
            )

            file_download >> extract_dates >> upload_to_s3
        
        else: 
            file_download >> upload_to_s3









 
