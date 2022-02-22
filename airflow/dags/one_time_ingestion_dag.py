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
from sqlalchemy import table
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator


path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
S3_DESTINATION = "raw/cycling-extras"
S3_BUCKET = os.environ.get("S3_BUCKET", "s3_no_bucket")
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


def initialize_workflow(**kwargs):
    
    # add BEGIN to mark the start of future queries in the script
    kwargs['ti'].xcom_push(
        key="ddl_query", 
        value='''
        -- DDL QUERY TO CREATE STAGING TABLES
        BEGIN;
        ''')

# infer schema for from a file
def infer_table_schema(filepath, **kwargs):

    file_type= filepath.split('.')[-1]
    
    fnm= filepath.split('/')[-1]

    table_name= fnm.split('.')[0]
    
    
    if file_type == 'csv':
        df= pd.read_csv(filepath)
    
    elif file_type == 'json':
        df= pd.read_json(filepath)
    
    else:
        raise Exception(f"Sorry, the filetype {file_type} is not supported.")
    
    schema= pd.io.sql.get_schema(frame=df, name=f'staging_{table_name}')
    if 'CREATE TABLE' in schema:
        # add IF NOT EXISTS condition in the DDL query
        schema= schema.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
        
    else:
        raise Exception(f"Sorry, the schema: {schema} does not seem to be correct.")
    
    ddl_query= kwargs['ti'].xcom_pull(key='ddl_query')
    ddl_query+= f"""
    
    -- CREATE staging_{table_name} TABLE
    {schema};
    -- END OF TABLE CREATION

    """
    
    print(ddl_query)

    kwargs['ti'].xcom_push(key="ddl_query", value=ddl_query)


def print_schema(**kwargs):

    ddl_query= kwargs['ti'].xcom_pull(key='ddl_query')

    # add END to terminate the script
    ddl_query+='''
    END;
    '''
    kwargs['ti'].xcom_push(key="ddl_query", value=ddl_query)
    print('SCHEMA:', ddl_query)



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

    start = PythonOperator(
                task_id="start",
                python_callable=initialize_workflow,
                provide_context=True
            )

    for index, item in enumerate(download_links):
        
        with TaskGroup(f"{item['name']}_data", tooltip="Download - Upload") as item['name']:
            download_task = BashOperator(
                task_id=f"download_{index}_task",
                bash_command=f"wget {item['link']} -O {path_to_local_home}/{item['output']}"
            )


            preprocessing_task = PythonOperator(
                task_id=f"preprocess_{index}_data",
                python_callable=preprocess_data,
                provide_context=True,
                op_kwargs={
                    "filepath": f"{path_to_local_home}/{item['output']}"
                }
            )

            infer_schema_task = PythonOperator(
                task_id=f"infer_{index}_schema",
                python_callable=infer_table_schema,
                provide_context=True,
                op_kwargs={
                    "filepath": f"{path_to_local_home}/{item['output']}",
                }
            )
            
            upload_to_s3 = LocalFilesystemToS3Operator(
                task_id=f"upload_to_s3_task",
                filename=item['output'],
                dest_key=f"{S3_DESTINATION}/{item['output']}",
                dest_bucket=S3_BUCKET,
            )
            
            download_task >> preprocessing_task >> infer_schema_task >> upload_to_s3
            

    print_the_schema = PythonOperator(
        task_id="print_tables_schema_task",
        python_callable=print_schema,
        provide_context=True
    )


    create_redshift_tables = RedshiftSQLOperator(
        task_id="create_staging_tables_task",
        sql="{{ti.xcom_pull(key='ddl_query')}}"
    )


    cleanup = BashOperator(
        task_id="cleanup_local_storage",
        bash_command=f"rm {path_to_local_home}/*.json {path_to_local_home}/*.csv "
    )

    start >> [item['name'] for item in download_links] >> print_the_schema >> create_redshift_tables >> cleanup
