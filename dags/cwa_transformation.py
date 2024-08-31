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

@task(task_id='list_s3_snowflake_diff')
def list_s3_snowflake_diff(ti, **context):
    s3_id = Variable.get('s3-side-project-id')
    s3_key = Variable.get('s3-side-project-key')
    s3_bucket_name = Variable.get('s3-dev-bucket-name')
    s3_region_name = Variable.get('s3-default-region')
    
    try:
        s3 = boto3.client(
            's3',
            region_name=s3_region_name,
            aws_access_key_id=s3_id,
            aws_secret_access_key=s3_key,
        )
        
        objects = s3.list_objects_v2(Bucket=s3_bucket_name)
        filelist = [obj for obj in objects['Contents']]
        print(filelist)
        
    except ClientError as e:
        print(e)
        raise e


with DAG(
    dag_id='cwa_transformation_v_0_1_0',
    start_date=datetime(2024,1,1),
    catchup=False,
    schedule="1 */2 * * *",
):
    
    list_s3_snowflake_diff_task = list_s3_snowflake_diff()
    
    list_s3_snowflake_diff_task