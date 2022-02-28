import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

S3_BUCKET = os.environ.get("S3_BUCKET", "s3_no_bucket")
local_scripts = "dags/scripts"
s3_script = "utils/scripts/"

SPARK_STEPS = [
     {
        "Name": "Move scripts from S3 to HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT", 
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://{{ params.S3_BUCKET }}/{{ params.s3_script }}",
                "--dest=/source",
            ],
        },
    },
    {
        "Name": "One-time data transformation",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "/source/{{ params.s3_script }}/one-time-data-transformation.py",
            ],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'ExtrasDataTransformer',
    'ReleaseLabel': 'emr-5.29.0',
    'Applications': [{'Name': 'Spark'}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Primary node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm1.medium',
                'InstanceCount': 1,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    'Steps': SPARK_STEPS,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}



default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="one_time_spark_emr_dag",
    description="""
        This dag perform a manually triggered and one-time-running workflow which processes extra files in s3.
    """, 
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['spark', 'emr', 'weather', 'stations', 'docking stations', 'london', '2021', 'journey'],
) as dag:

    start = DummyOperator(task_id="start")
    
    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )

    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id=cluster_creator.output,
        steps=SPARK_STEPS,
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id=cluster_creator.output,
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
    )

    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster', job_flow_id=cluster_creator.output
    )

    
    end = DummyOperator(task_id="end")

    start >> step_adder >> step_checker >> cluster_remover >> end