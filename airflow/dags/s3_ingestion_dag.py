import os

import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.utils.dates import datetime

import pyarrow.csv as pv
import pyarrow.parquet as pq

# https://cycling.data.tfl.gov.uk/usage-stats/cycling-load.json

dataset_file = "cycling-load.json"
dataset_url = f"https://cycling.data.tfl.gov.uk/usage-stats/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
destination= "raw/test"
S3_BUCKET = os.environ.get("S3_BUCKET", "s3_no_bucket")


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="example_local_to_s3",
    schedule_interval="@daily", 
    max_active_runs=1,
    catchup=False,
    tags=['s3', 'aws', 'ingestion', 'cycling'],
    default_args=default_args
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    
    upload_to_s3_task = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename=f"{path_to_local_home}/{dataset_file}",
        dest_key=f"{destination}/{dataset_file}",
        dest_bucket=S3_BUCKET,
    )

    cleanup_local_storage_task = BashOperator(
        task_id="cleanup_local_storage_task",
        bash_command=f"rm {path_to_local_home}/{dataset_file}"
    )

    download_dataset_task >> upload_to_s3_task >> cleanup_local_storage_task