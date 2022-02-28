import os

import logging
import json
import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
S3_DESTINATION = "raw/cycling-extras"
S3_BUCKET = os.environ.get("S3_BUCKET", "s3_no_bucket")
S3_SCRIPT_DESTINATION = "utils/scripts/"
download_links= [
    {   
        'name': 'stations',
        'link': 'https://www.whatdotheyknow.com/request/664717/response/1572474/attach/3/Cycle%20hire%20docking%20stations.csv.txt',
        'output': 'stations.csv'
    },
    {
        'name': 'weather',
        'link': '--no-check-certificate "https://docs.google.com/uc?export=download&id=1Aa2mP5CwLele94GkJWqvpCmlm6GXeu8c"',
        'output': 'weather.json'
    },
    {
        'name': 'journey',
        'link': 'https://cycling.data.tfl.gov.uk/usage-stats/246JourneyDataExtract30Dec2020-05Jan2021.csv',
        'output': 'journey.csv'
    }
]
local_scripts = [ 'journey-transformation.py', 'one-time-data-transformation.py' ]


# extract days value from the weather data
def preprocess_data(filepath):

    filename= filepath.split('/')[-1]

    if filename != 'weather.json':
        print(f'No preprocessing needed for {filename}')
        return
    
    with open(filepath, 'r') as f:
        weather = json.load(f)

    daily_weather= weather['days']
    
    with open(filepath, 'w') as f:
        json.dump(daily_weather, f)


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
        This dag ingests extra files for the cycling journey including: the docking stations, 
        the weather data and an example file for cycling journey.
    """, 
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['weather', 'stations', 'docking stations', 'london', '2021', 'journey'],
) as dag:

    start = DummyOperator(task_id="start")

        
    with TaskGroup(f"Download_files", tooltip="Download - Preprocess") as download_section:

        for index, item in enumerate(download_links):
            download_task = BashOperator(
                task_id=f"download_{item['name']}_task",
                bash_command=f"wget {item['link']} -O {path_to_local_home}/{item['output']}"
            )

            if item['output'] == 'weather.json':
                preprocessing_task = PythonOperator(
                    task_id=f"extract_daily_weather_data",
                    python_callable=preprocess_data,
                    provide_context=True,
                    op_kwargs={
                        "filepath": f"{path_to_local_home}/{item['output']}"
                    }
                )

                download_task >> preprocessing_task
            


    with TaskGroup("upload_files_to_s3") as upload_section:

        for index, item in enumerate(download_links):
            
            upload_to_s3_task = LocalFilesystemToS3Operator(
                task_id=f"upload_{item['name']}_to_s3_task",
                filename=item['output'],
                dest_key=f"{S3_DESTINATION}/{item['output']}",
                dest_bucket=S3_BUCKET,
            )


    cleanup = BashOperator(
        task_id="cleanup_local_storage",
        bash_command=f"rm {path_to_local_home}/*.json {path_to_local_home}/*.csv "
    )

    # upload scripts
    with TaskGroup("upload_scripts_to_s3") as upload_scripts_section:
        for index, item in enumerate(local_scripts):
            upload_scripts_to_s3_task = LocalFilesystemToS3Operator(
                task_id=f"upload_scritps_{index}_to_s3_task",
                filename=item,
                dest_key=f"{S3_DESTINATION}/{item}",
                dest_bucket=S3_BUCKET,
            )

    end = DummyOperator(task_id="end")

    start >> download_section >> upload_section >> cleanup >> end
    start >> upload_scripts_section >> end

