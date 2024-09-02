import json
import boto3
import snowflake.connector

from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

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

from rich.progress import track

dbt_project_path = Path("/opt/airflow/dags/repo/dags/dbt_snowflake")

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
def snowflake_preflight_check(ti, **context):
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
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {snowflake_schema_cwb}_intermediate;")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {snowflake_schema_cwb}_transformerd;")
        
        cursor.execute(f"CREATE TABLE {snowflake_schema_cwb}.raw IF NOT EXISTS (filename string, raw_data VARIANT);")
        
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
        print(f"{s3_filelist=}")
        
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
        cursor.execute(f"SELECT filename FROM {snowflake_schema_cwb}.raw")
        snowflake_record_list = [item[0] for item in cursor]
        print(f"{snowflake_record_list=}")
    except Exception as e:
        print(e)
        raise e
    
    diff = list(
        set(s3_filelist)
        .difference(set(snowflake_record_list))
    )

    print(f"Found number of different files: {len(diff)}")
    print(f"{diff=}")
    return diff

@task(task_id='upload_snowflake')
def upload_snowflake(ti, **context):
    to_upload_files = ti.xcom_pull(task_ids='list_s3_snowflake_diff')
    
    
    snowflake_user = Variable.get('snowflake_username')
    snowflake_password = Variable.get('snowflake_password')
    snowflake_account = Variable.get('snowflake_account')
    
    snowflake_database = Variable.get('snowflake_database')
    snowflake_schema_cwb = Variable.get('snowflake_schema_cwb_dev')
    snowflake_stage_name = Variable.get('snowflake_dev_stage_name')
    
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
        )
        
        snowflake_conn.cursor().execute(f"USE DATABASE {snowflake_database};")
        
        for file in track(to_upload_files, total=len(to_upload_files), description='Uploading to snowflake table.'):
            print(f"Uploading {file}.")
            snowflake_conn.cursor().execute(
                f"""
                COPY INTO {snowflake_schema_cwb}.raw
                FROM (
                    SELECT 
                        '{file}', $1
                    FROM
                        @{snowflake_stage_name}/{file}
                    )
                    FILE_FORMAT=(TYPE='JSON')
                ;
                """
            )
        
    except Exception as e:
        print(e)
        raise e
    

with DAG(
    dag_id='cwa_transformation_v_0_2_0',
    start_date=datetime(2024,1,1),
    catchup=False,
    schedule="1 */2 * * *",
):
    snowflake_preflight_check_task = snowflake_preflight_check()
    list_s3_snowflake_diff_task = list_s3_snowflake_diff()
    upload_snowflake_task = upload_snowflake()
    
    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(dbt_project_path),
        operator_args={
            "install_deps": True,
            "dbt_cmd_flags": ["--target-path"," /opt/airflow/new_target"]
        },
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=f"/opt/airflow/dbt_venv/bin/dbt",),
    )
    
    snowflake_preflight_check_task >> list_s3_snowflake_diff_task >> upload_snowflake_task >> transform_data