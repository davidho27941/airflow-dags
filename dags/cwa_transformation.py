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

dbt_project_path = Path("/opt/airflow/dags/dbt_snowflake")

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_dev", 
        profile_args={
            "database": "weather_data",
            "schema": "cwb_dev"
        },
))


@task(task_id='snowflake_preflight_check')
def snowflake_preflight_check():
    snowflake_user = Variable.get('snowflake_username')
    snowflake_password = Variable.get('snowflake_password')
    snowflake_account = Variable.get('snowflake_account')
    
    snowflake_database = Variable.get('snowflake_database')
    snowflake_schema_cwb = Variable.get('snowflake_schema_cwb_dev')
    snowflake_stage_name = Variable.get('snowflake_stage_name')
    
    try:
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
        cursor = snowflake_conn.cursor()
        
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {snowflake_schema_cwb};")
        cursor.execute(f"CREATE TABLE {snowflake_schema_cwb}.raw IF NOT EXISTS (log_time string, raw_data VARIANT);")
        
    except Exception as e:
        print(e)
        raise e
    

@task(task_id='list_s3_snowflake_diff')
def list_s3_snowflake_diff(ti, **context):
    s3_id = Variable.get('s3-side-project-id')
    s3_key = Variable.get('s3-side-project-key')
    s3_bucket_name = Variable.get('s3-dev-bucket-name')
    s3_region_name = Variable.get('s3-default-region')

    snowflake_user = Variable.get('snowflake_username')
    snowflake_password = Variable.get('snowflake_password')
    snowflake_account = Variable.get('snowflake_account')
    
    snowflake_database = Variable.get('snowflake_database')
    snowflake_schema_cwb = Variable.get('snowflake_schema_cwb_dev')
    snowflake_stage_name = Variable.get('snowflake_stage_name')
    
    
    try:
        s3 = boto3.client(
            's3',
            region_name=s3_region_name,
            aws_access_key_id=s3_id,
            aws_secret_access_key=s3_key,
        )
        
        objects = s3.list_objects_v2(Bucket=s3_bucket_name)
        s3_filelist = [obj['Key'] for obj in objects['Contents']]
        print(s3_filelist)
        
    except ClientError as e:
        print(e)
        raise e

    try:
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
        cursor = snowflake_conn.cursor()
        cursor.execute(f"SELECT log_time FROM {snowflake_schema_cwb}.raw")
        snowflake_record_list = [item for item in cursor]
        print(snowflake_record_list)
    except Exception as e:
        print(e)
        raise e
    
    diff = set(s3_filelist).difference(set(snowflake_record_list))
    
    print(f"{diff=}")
        

with DAG(
    dag_id='cwa_transformation_v_0_1_0',
    start_date=datetime(2024,1,1),
    catchup=False,
    schedule="1 */2 * * *",
):
    snowflake_preflight_check_task = snowflake_preflight_check()
    list_s3_snowflake_diff_task = list_s3_snowflake_diff()
    
    snowflake_preflight_check_task >> list_s3_snowflake_diff_task