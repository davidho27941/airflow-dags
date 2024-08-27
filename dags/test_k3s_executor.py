import json
import boto3
import snowflake.connector

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator

from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

from airflow.providers.http.operators.http import HttpOperator

from datetime import datetime, timedelta

from botocore.exceptions import ClientError

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from pathlib import Path

@task(task_id='python_test')
def python_test(ti, **context):
    print('Available.')

with DAG(
    dag_id='test_k3s',
    start_date=datetime(2024,1,1),
    catchup=False,
    schedule="1/10 * * * *",
):
    token = Variable.get('cwa_auth_token')
    ping_task = HttpOperator(
        task_id='ping_cwa_api_task',
        http_conn_id='cwa_real_time_api',
        endpoint="/api/v1/rest/datastore/O-A0003-001",
        method='GET',
        data={
            'Authorization': f'{token}',
            'format': "JSON",
        },
        headers={"Content-Type": "application/json"},
        log_response=True,
    )

    python_test = python_test()

    ping_task >> python_test
