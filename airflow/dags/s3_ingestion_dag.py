import os

import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.utils.dates import datetime


import json

# https://cycling.data.tfl.gov.uk/usage-stats/cycling-load.json

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
S3_DESTINATION= 'raw/cycling-journey/{{logical_date.strftime(\'%b%Y\')}}'
S3_BUCKET = os.environ.get("S3_BUCKET", "s3_no_bucket")
 
dictionary_file= "links_dictionary.json"


def get_file_link(exec_date, s3_destination_folder, **kwargs):
    links= {}
    with open(dictionary_file) as dico_file:
        links= json.load(dico_file)
    
    file_link= links[exec_date]
    filename= file_link.split('/')[-1]
    
    kwargs['ti'].xcom_push(key="remote_file_link", value=file_link)
    kwargs['ti'].xcom_push(key="filename", value=filename)
    kwargs['ti'].xcom_push(key="local_file_link", value=f"{path_to_local_home}/{filename}")
    kwargs['ti'].xcom_push(key="s3_filepath_destination", value=f"{s3_destination_folder}/{filename}")
    

download_cmd= "curl -sSLf $link > $destination"


default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 1),
    "depends_on_past": True,  # the previous task instance needs to have succeeded for the current one to run
    "retries": 1,
}

with DAG(
    dag_id="s3_ingestion_dag",
    schedule_interval="55 23 * * 2",  # run this dag every Tuesday at 11:55pm
    max_active_runs=3,
    catchup=True,
    tags=['s3', 'aws', 'ingestion', 'cycling'],
    default_args=default_args
) as dag:

    get_file_link_task = PythonOperator(
        task_id="get_file_link_task",
        provide_context=True,
        python_callable=get_file_link,
        op_kwargs={
            "exec_date": "{{execution_date.strftime('%d%b%Y')}}",
            "s3_destination_folder": S3_DESTINATION
        }
    )


    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=download_cmd,
        env={
            "link": "{{ti.xcom_pull(key='remote_file_link')}}",
            "destination": "{{ti.xcom_pull(key='local_file_link')}}"
        }
    )

    
    upload_to_s3_task = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename="{{ti.xcom_pull(key='filename')}}",
        dest_key="{{ti.xcom_pull(key='s3_filepath_destination')}}",
        dest_bucket=S3_BUCKET,
    )

    cleanup_local_storage_task = BashOperator(
        task_id="cleanup_local_storage_task",
        bash_command="rm {{ti.xcom_pull(key='local_file_link')}}"
    )

    get_file_link_task >> download_dataset_task >> upload_to_s3_task >> cleanup_local_storage_task