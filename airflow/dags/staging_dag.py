import os

import logging
import json

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


S3_BUCKET = os.environ.get("S3_BUCKET", "s3_no_bucket")
S3_KEY_EXTRAS = f"raw/cycling-extras"
S3_KEY_JOURNEY = f"raw/cycling-journey"

s3_objects= [
    {   
        'type': 'stations',
        'key': S3_KEY_EXTRAS,
        'filename': 'stations.csv'
    },
    {
        'type': 'weather',
        'key': S3_KEY_EXTRAS,
        'filename': 'weather.json'
    },
    {
        'type': 'journey',
        'key': f'{S3_KEY_JOURNEY}/',
        'filename': '246JourneyDataExtract30Dec2020-05Jan2021.parquet'
    }
]






