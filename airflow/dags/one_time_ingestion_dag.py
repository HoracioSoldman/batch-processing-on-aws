import os

import logging
import json

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
S3_DESTINATION = f"raw/cycling-extras"
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
def extract_dates(filepath):
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
    max_active_runs=3,
    tags=['weather', 'stations', 'docking stations', 'london', '2021'],
) as dag:

    start_task = DummyOperator(
        task_id="start"
    )

    with TaskGroup("download_extract_weather_data", tooltip="Download - Extract") as section_weather:
        download_w = BashOperator(
            task_id="download_weather_task",
            bash_command=f"wget {download_links[1]['link']} -O {path_to_local_home}/{download_links[1]['output']}"
        )

        extract_daily_weather = PythonOperator(
            task_id="extract_daily_weather_task",
            python_callable=extract_dates,
            op_kwargs={
                "filepath": f"{path_to_local_home}/{download_links[1]['output']}"
            }
        )

        download_w >> extract_daily_weather

    

    download_s = BashOperator(
        task_id="download_stations_task",
        bash_command=f"wget {download_links[0]['link']} -O {path_to_local_home}/{download_links[0]['output']}"
    )
    
    with TaskGroup("upload_files_to_s3", tooltip="Upload to S3") as section_upload:
        for item in download_links:
            upload_to_s3 = LocalFilesystemToS3Operator(
                task_id=f"upload_{item['output']}_to_s3_task",
                filename=item['output'],
                dest_key=S3_DESTINATION,
                dest_bucket=S3_BUCKET,
            )

    

    end_task = BashOperator(
        task_id="end",
        bash_command=f"rm {path_to_local_home}/*.json {path_to_local_home}/*.csv "
    )


    start_task >> [download_s, section_weather] >> section_upload >> end_task

