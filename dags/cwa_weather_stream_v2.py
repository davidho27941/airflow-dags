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

from datetime import datetime, timedelta

from botocore.exceptions import ClientError

from pathlib import Path


# @task.branch(task_id='check_bucket_existence')
# def check_bucket_existence(ti, **context):
#     s3_id = Variable.get('s3-side-project-id')
#     s3_key = Variable.get('s3-side-project-key')
#     s3_bucket_name = Variable.get('s3-dev-bucket-name')
#     s3_region_name = Variable.get('s3-default-region')
    
#     s3 = boto3.client(
#         's3',
#         region_name=s3_region_name,
#         aws_access_key_id=s3_id,
#         aws_secret_access_key=s3_key,
#     )
    
#     response = s3.list_buckets()
    
#     print(response['Buckets'])

#     if s3_bucket_name in response['Buckets']:
#         return 'upload_s3'
#     else: 
#         return 'create_bucket'


# @task(task_id='create_bucket')
# def create_bucket(ti, **context):
#     s3_id = Variable.get('s3-side-project-id')
#     s3_key = Variable.get('s3-side-project-key')
#     s3_bucket_name = Variable.get('s3-dev-bucket-name')
#     s3_region_name = Variable.get('s3-default-region')
    
#     try:
#         s3 = boto3.client(
#             's3',
#             region_name=s3_region_name,
#             aws_access_key_id=s3_id,
#             aws_secret_access_key=s3_key,
#         )
        
#         s3.create_bucket(Bucket=s3_bucket_name)
        
#     except ClientError as e:
#         print(e)
#         raise e


    
@task(task_id='upload_s3_test', trigger_rule=TriggerRule.ONE_SUCCESS, retries=3, retry_delay=timedelta(minutes=1))
def upload_s3(ti, **context):
    s3_id = Variable.get('s3-side-project-id')
    s3_key = Variable.get('s3-side-project-key')
    s3_bucket_name = Variable.get('s3-dev-bucket-name')
    s3_region_name = Variable.get('s3-default-region')
    
    data = ti.xcom_pull(task_ids='ping_cwa_api_task')
    print(data)
    data = json.loads(data)
    
    s3 = boto3.client(
        's3',
        region_name=s3_region_name,
        aws_access_key_id=s3_id,
        aws_secret_access_key=s3_key,
    )
    
    try:
        # Follow the solution provided by: https://stackoverflow.com/questions/46844263/writing-json-to-file-in-s3-bucket
        # Alternative method: https://repost.aws/questions/QUemVDeKUTRm-KL7DjjHFtSA/uploading-a-file-to-s3-using-python-boto3-and-codepipeline
        timestamp = f"{datetime.now(pytz.timezone('Asia/Taipei')):%Y-%m-%d_%H_%M}"
        timestamp_to_pass = f"{datetime.now(pytz.timezone('Asia/Taipei')):%Y-%m-%d %H:%M}"
        
        file_key = f'weather_record/weather_report_10min-{timestamp}_v2_alt.json'
        s3.put_object(
            Body=json.dumps(data),
            Bucket=s3_bucket_name,
            Key=file_key
        )
        info = (file_key, timestamp_to_pass)
        return info
        
    except ClientError as e:
        print(e)
        raise e

with DAG(
    dag_id='cwa_weather_stream_v_2_0_0',
    start_date=datetime(2024,1,1),
    catchup=False,
    schedule="1/10 * * * *",
):
    
    token = Variable.get('cwa_auth_token')
    s3_bucket_name = Variable.get('s3-dev-bucket-name')
    
    # ping_task = HttpOperator(
    #     task_id='ping_cwa_api_task',
    #     http_conn_id='cwa_real_time_api',
    #     endpoint="/api/v1/rest/datastore/O-A0003-001",
    #     method='GET',
    #     data={
    #         'Authorization': f'{token}',
    #         'format': "JSON",
    #     },
    #     headers={"Content-Type": "application/json"},
    #     log_response=True,
    # )

    
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
        # s3_bucket=f"s3://{s3_bucket_name}",
        s3_key=f"s3://{s3_bucket_name}/weather_record/weather_report_10min-{{ execution_date }}_v2.json",
        aws_conn_id="aws_s3_conn",
    )

    get_recent_weather_task

    # [ping_task, get_recent_weather_task] >> upload_s3()


    # check_bucket_existence_task = check_bucket_existence()
    # create_bucket_task = create_bucket()
    # upload_s3_task = upload_s3()
    
    # ping_task >> check_bucket_existence_task
    # check_bucket_existence_task >> create_bucket_task >> upload_s3_task
    # check_bucket_existence_task >> upload_s3_task