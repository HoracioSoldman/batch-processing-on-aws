import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.sensors.external_task import ExternalTaskSensor


S3_BUCKET = os.environ.get("S3_BUCKET", "s3_no_bucket")
S3_KEY_EXTRAS = f"processed/cycling-dimension"
S3_KEY_JOURNEY = f"processed/test"

s3_objects= [
    # we will only load the processed weather dimensional data in this dag, 
    # as the stations and datetime data still receives updates from proc_2_spark_emr_dag.py.
    {
        'type': 'weather',
        'key': S3_KEY_EXTRAS,
        'filename': 'weather/',
        'table': 'dim_weather',
        'file_type': 'parquet',
        'upsert_key': 'weather_date'
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
    dag_id="init_2_s3_to_redshifht_dag",
    description="""
        This dag transfers extra files for dimensions from S3 to Redshift.
    """, 
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['weather', 'stations', '2021', 's3 to redshift'],
) as dag:


    external_task_sensor = ExternalTaskSensor(
        task_id='sensor_for_init_1_spark_dag',
        poke_interval=30,
        soft_fail=False,
        retries=2,
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        external_task_id='end',
        external_dag_id='init_1_spark_emr_dag',
    )

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

    external_task_sensor >> start >> transfer_section >> end
