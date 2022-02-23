import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


S3_BUCKET = os.environ.get("S3_BUCKET", "s3_no_bucket")
S3_KEY_EXTRAS = f"raw/cycling-extras"
S3_KEY_JOURNEY = f"raw/cycling-journey"

s3_objects= [
    {   
        'type': 'stations',
        'key': S3_KEY_EXTRAS,
        'filename': 'stations.csv',
        'table': 'staging_stations',
        'file_type': 'CSV'
    },
    {
        'type': 'weather',
        'key': S3_KEY_EXTRAS,
        'filename': 'weather.json',
        'table': 'staging_weather',
        'file_type': 'JSON'
    },
    {
        'type': 'journey',
        'key': S3_KEY_EXTRAS,
        'filename': 'journey.csv',
        'table': 'staging_journey',
        'file_type': 'CSV'
    }
]


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="one_time_staging_dag",
    description="""
        This dag transfers all the files from S3 to Redshift.
    """, 
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['weather', 'stations', '2021', 's3 to redshift'],
) as dag:

    start = DummyOperator(task_id="start")
    
    for item in s3_objects:
        item['type'] = S3ToRedshiftOperator(
            s3_bucket=S3_BUCKET,
            s3_key=f"{item['key']}/{item['filename']}",
            schema="PUBLIC",
            table=item['table'],
            copy_options=[item['file_type']],
            method='UPSERT',
            task_id=f"transfer_{item['type']}_s3_to_redshift",
        )

    end = DummyOperator(task_id="end")

    start >> s3_objects[0]['type'] >> s3_objects[1]['type'] >> end
    start >> s3_objects[2]['type'] >> end












