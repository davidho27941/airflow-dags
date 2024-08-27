import json
import boto3
import snowflake.connector

from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator

from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

from airflow.providers.http.operators.http import HttpOperator
# from airflow.providers.mongo.hooks.mongo import MongoHook
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from datetime import datetime, timedelta

from botocore.exceptions import ClientError

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from pathlib import Path

# dbt_project_path = Path("/opt/airflow/dags/dbt_snowflake")

# profile_config = ProfileConfig(
#     profile_name="default",
#     target_name="dev",
#     profile_mapping=SnowflakeUserPasswordProfileMapping(
#         conn_id="snowflake_default", 
#         profile_args={
#             "database": "weather_data",
#             "schema": "cwb"
#         },
# ))



@task.branch(task_id='check_bucket_existence')
def check_bucket_existence(ti, **context):
    s3_id = Variable.get('s3-side-project-id')
    s3_key = Variable.get('s3-side-project-key')
    s3_bucket_name = Variable.get('s3-default-bucket-name')
    s3_region_name = Variable.get('s3-default-region')
    
    s3 = boto3.client(
        's3',
        region_name=s3_region_name,
        aws_access_key_id=s3_id,
        aws_secret_access_key=s3_key,
    )
    
    response = s3.list_buckets()

    if s3_bucket_name in response['Buckets']:
        return 'upload_s3'
    else: 
        return 'create_bucket'


@task(task_id='create_bucket')
def create_bucket(ti, **context):
    s3_id = Variable.get('s3-side-project-id')
    s3_key = Variable.get('s3-side-project-key')
    s3_bucket_name = Variable.get('s3-default-bucket-name')
    s3_region_name = Variable.get('s3-default-region')
    
    try:
        s3 = boto3.client(
            's3',
            region_name=s3_region_name,
            aws_access_key_id=s3_id,
            aws_secret_access_key=s3_key,
        )
        
        s3.create_bucket(Bucket=s3_bucket_name)
        
    except ClientError as e:
        print(e)
        raise e


    
@task(task_id='upload_s3', trigger_rule=TriggerRule.ONE_SUCCESS, retries=3, retry_delay=timedelta(minutes=1))
def upload_s3(ti, **context):
    s3_id = Variable.get('s3-side-project-id')
    s3_key = Variable.get('s3-side-project-key')
    s3_bucket_name = Variable.get('s3-default-bucket-name')
    s3_region_name = Variable.get('s3-default-region')
    
    data = ti.xcom_pull(task_ids='ping_cwa_api_task')
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
        timestamp = f"{datetime.now():%Y-%m-%d_%H_%M}"
        timestamp_to_pass = f"{datetime.now():%Y-%m-%d %H:%M}"
        
        file_key = f'weather_report_10min-{timestamp}.json'
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

@task(task_id='s3_to_snowflake')
def s3_to_snowflake(info):
    filename, timestamp = info
    print(filename, timestamp)
    snowflake_user = Variable.get('snowflake_username')
    snowflake_password = Variable.get('snowflake_password')
    snowflake_account = Variable.get('snowflake_account')
    
    snowflake_database = Variable.get('snowflake_database')
    snowflake_schema_cwb = Variable.get('snowflake_schema_cwb')
    snowflake_stage_name = Variable.get('snowflake_stage_name')
    
    
    snowflake_conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        database=snowflake_database,
        schema=snowflake_schema_cwb,
        stage=snowflake_stage_name,
        warehouse = 'COMPUTE_WH',
        region='ap-northeast-1.aws',
        # role='ACCOUNTADMIN'
    )
    
    snowflake_conn.cursor().execute(f"USE DATABASE {snowflake_database};")
    snowflake_conn.cursor().execute(f"CREATE TABLE  {snowflake_schema_cwb}.raw IF NOT EXISTS(log_time string, raw_data VARIANT);")
    snowflake_conn.cursor().execute(
        f"""
        COPY INTO {snowflake_schema_cwb}.raw
        FROM (
            SELECT 
                '{timestamp}', $1
            FROM
                @{snowflake_stage_name}/{filename}
            )
            FILE_FORMAT=(TYPE='JSON')
        ;
        """
    )

    snowflake_conn.close()

with DAG(
    dag_id='cwa_weather_stream_v_1_0_0',
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

    # transform_data = DbtTaskGroup(
    #     group_id="transform_data",
    #     project_config=ProjectConfig(dbt_project_path),
    #     operator_args={"install_deps": True},
    #     profile_config=profile_config,
    #     execution_config=ExecutionConfig(dbt_executable_path=f"/opt/airflow/dbt_venv/bin/dbt",),
    # )

    check_bucket_existence_task = check_bucket_existence()
    create_bucket_task = create_bucket()
    upload_s3_task = upload_s3()
    # s3_to_snowflake_task = s3_to_snowflake(upload_s3_task)
    
    ping_task >> check_bucket_existence_task
    check_bucket_existence_task >> create_bucket_task >> upload_s3_task
    check_bucket_existence_task >> upload_s3_task
    
    # upload_s3_task >> s3_to_snowflake_task
    # s3_to_snowflake_task >> transform_data
