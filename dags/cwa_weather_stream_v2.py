import json
import pytz
import boto3
import snowflake.connector

from airflow import DAG

from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.amazon.aws.transfers.http_to_s3 import HttpToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from datetime import datetime, timedelta

from botocore.exceptions import ClientError

from pathlib import Path


with DAG(
    dag_id='cwa_weather_stream_v_2_0_0',
    start_date=datetime(2024,1,1),
    catchup=False,
    schedule="1/10 * * * *",
):
    
    token = Variable.get('cwa_auth_token')
    s3_bucket_name = Variable.get('s3-dev-bucket-name')
    
    
    get_recent_weather_task = HttpToS3Operator(
        task_id='get_weather_to_s3',
        http_conn_id="cwa_real_time_api",
        endpoint="/api/v1/rest/datastore/O-A0003-001",
        method="GET",
        data={
            'Authorization': f'{token}',
            'format': "JSON",
        },
        headers={"Content-Type": "application/json"},
        log_response=True,
        s3_bucket=f"{s3_bucket_name}",
        s3_key="weather_record/weather_report_10min-{{ execution_date }}_v2.json",
        aws_conn_id="aws_s3_conn",
    )

    s3_to_redshift = S3ToRedshiftOperator(
        task_id='load_s3_weather_into_redshift',
        redshift_conn_id="weather_redshift_conn",
        aws_conn_id="aws_s3_conn",
        s3_bucket=f"{s3_bucket_name}",
        s3_key="weather_record/weather_report_10min-{{ execution_date }}_v2.json",
        schema="public",
        table="weather_test",
        copy_options=["JSON 's3://side-project-dev/manifests/jsonpaths/weather/weather_10_min_jsonpaths.json'"],
        method="APPEND",
    )

    get_recent_weather_task >> s3_to_redshift
