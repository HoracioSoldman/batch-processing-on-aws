from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.sensors.external_task import ExternalTaskSensor


SPARK_STEPS = [
    {
        "Name": "One-time data transformation",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://hrc-de-data/utils/scripts/init-data-transformation.py",
            ],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'ExtrasDataTransformer',
    'ReleaseLabel': 'emr-5.34.0',
    'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}],
    'LogUri': 's3n://hrc-de-data/emr/logs',
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Primary node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
    },
    'Steps': SPARK_STEPS,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}



default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="init_1_spark_emr_dag",
    description="""
        This dag perform a manually triggered and one-time-running spark jobs which processes extra files in s3.
    """, 
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['spark', 'emr', 'weather', 'stations', 'docking stations', 'london', '2021', 'journey'],
) as dag:


    external_task_sensor = ExternalTaskSensor(
        task_id='sensor_for_init_0_ingestion_dag',
        poke_interval=30,
        soft_fail=False,
        retries=2,
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        external_task_id='end',
        external_dag_id='init_0_ingestion_to_s3_dag',
    )

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
        task_id='remove_cluster', job_flow_id=cluster_creator.output,
        
    )

    
    end = DummyOperator(task_id="end")

    external_task_sensor >> start >> cluster_creator >> step_adder >> step_checker  >> cluster_remover >> end