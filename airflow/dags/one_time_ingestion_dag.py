import os

import logging
import json

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator
from airflow.utils.dates import datetime

import pyarrow.csv as pv
import pyarrow.parquet as pq
import pandas as pd


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
    },
    {
        'type': 'journey',
        'link': 'https://cycling.data.tfl.gov.uk/usage-stats/252JourneyDataExtract10Feb2021-16Feb2021.csv',
        'output': 'journey.csv'
    },

]

# extract days value from the weather data
def extract_dates(filepath):
    filepath= f"{path_to_local_home}/{download_links[1]['output']}"
    
    with open(filepath, 'r') as f:
        weather = json.load(f)

    days= weather['days']
    
    with open(filepath, 'w') as f:
        json.dump(days, f)

# infer schema for from a file
def infer_table_schema(source_file, table_name, **kwargs):
    file_type= source_file.split('.')[-1]
    if file_type == 'csv':
        df= pd.read_csv(source_file)
    elif file_type == 'json':
        df= pd.read_json(source_file)
    else:
        raise Exception(f"Sorry, the filetype {file_type} is not supported.")
    
    schema= pd.io.sql.get_schema(frame=df, name=table_name)
    if 'CREATE TABLE' in schema:
        # add IF NOT EXISTS condition in the DDL query
        schema= schema.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
        kwargs['ti'].xcom_push(key="ddl_query", value=schema)
    else:
        raise Exception(f"Sorry, the schema: {schema} does not seem to be correct.")



def print_schema(schema):
    print('SCHEMA:', schema)


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
    tags=['weather', 'stations', 'docking stations', 'london weather', 'london 2021'],
) as dag:

    for item in download_links:
        
        download_file = BashOperator(
            task_id=f"download_{item['type']}_task",
            bash_command=f"wget {item['link']} -O {path_to_local_home}/{item['output']}"
        )

        
        infer_schema = PythonOperator(
            task_id=f"infer_{item['type']}_schema",
            python_callable=infer_table_schema,
            provide_context=True,
            op_kwargs={
                "source_file": f"{path_to_local_home}/{item['output']}",
                "table_name": f"staging_{item['type']}"
            }
        )


        # create_redshift_table = RedshiftSQLOperator(
        #     task_id=f"create_table_{item['type']}_task",
        #     sql="{{ti.xcom_pull(key='ddl_query')}}"
        # )

        print_the_schema = PythonOperator(
            task_id=f"print_table_{item['type']}_schema_task",
            python_callable=print_schema,
            op_kwargs={
                "schema": "{{ti.xcom_pull(key='ddl_query')}}"
            }
        )


        # upload_to_s3 = LocalFilesystemToS3Operator(
        #     task_id=f"upload_{item['type']}_to_s3_task",
        #     filename=item['output'],
        #     dest_key=f"{S3_DESTINATION}/{item['output']}",
        #     dest_bucket=S3_BUCKET,
        # )

        
        cleanup_local_storage_task = BashOperator(
            task_id=f"cleanup_local_{item['output']}_task",
            bash_command=f"rm {path_to_local_home}/{item['output']}"
        )


        if item['type'] == 'weather':

            extract_daily_weather = PythonOperator(
                task_id="extract_weather_dates_task",
                python_callable=extract_dates,
                op_kwargs={
                    "filepath": f"{path_to_local_home}/{item['output']}"
                }
            )

            download_file >> extract_daily_weather >> infer_schema >> print_the_schema >> cleanup_local_storage_task
        
        else: 
            download_file >> infer_schema >> print_the_schema >> cleanup_local_storage_task
