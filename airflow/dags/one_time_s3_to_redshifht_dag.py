import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


S3_BUCKET = os.environ.get("S3_BUCKET", "s3_no_bucket")
S3_KEY_EXTRAS = f"processed/cycling-extras"
S3_KEY_JOURNEY = f"processed"

s3_objects= [
    # {   
    #     'type': 'stations',
    #     'key': S3_KEY_EXTRAS,
    #     'filename': 'stations',
    #     'table': 'dim_station',
    #     'file_type': 'parquet',
    #     'upsert_key': 'station_id'
    # },
    # {
    #     'type': 'weather',
    #     'key': S3_KEY_EXTRAS,
    #     'filename': 'weather/',
    #     'table': 'dim_weather',
    #     'file_type': 'parquet',
    #     'upsert_key': 'weather_date'
    # },
    {
        'type': 'journey',
        'key': S3_KEY_JOURNEY,
        'filename': 'cycling-journey/',
        'table': 'fact_journey',
        'file_type': 'parquet',
        'upsert_key': 'rental_id'
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
    dag_id="one_time_redshift_loading_dag",
    description="""
        This dag transfers extra files for dimensions from S3 to Redshift.
    """, 
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['weather', 'stations', '2021', 's3 to redshift'],
) as dag:

    start = DummyOperator(task_id="start")

    with TaskGroup("load_files_to_redshift") as transfer_section:
        for item in s3_objects:
            transfer_task = S3ToRedshiftOperator(
                s3_bucket=S3_BUCKET,
                s3_key=f"{item['key']}/{item['filename']}",
                schema="PUBLIC",
                table=item['table'],
                copy_options=[item['file_type']],
                method='UPSERT',
                upsert_keys= [item['upsert_key']],
                task_id=f"transfer_{item['type']}_s3_to_redshift",
            )

    end = DummyOperator(task_id="end")

    start >> transfer_section >> end











