from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


RAW_BUCKET_NAME = os.getenv("RAW_BUCKET_NAME", "cust-churn-raw-data")
PROCESSED_BUCKET_NAME = os.getenv("PROCESSED_BUCKET_NAME", "cust-churn-processed-data")
ATHENA_RESULTS_BUCKET_NAME = os.getenv(
    "ATHENA_RESULTS_BUCKET_NAME", "cust-churn-athena-results"
)
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
GLUE_JOB_NAME = os.getenv("GLUE_JOB_NAME", "glue_autoetl")
GLUE_DATABASE = os.getenv("GLUE_DATABASE", "churn_db")
ATHENA_TABLE = os.getenv("ATHENA_TABLE", "cleaned_churn_data")


default_args = {
    "owner": "admin",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def check_new_files():
    s3 = S3Hook(aws_conn_id="aws_default")
    files = s3.list_keys(bucket_name=RAW_BUCKET_NAME)
    if not files:
        raise ValueError(f"No new files found in s3://{RAW_BUCKET_NAME}/")


dag = DAG(
    dag_id="churn_etl_to_athena",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "churn", "aws"],
)


with dag:
    check_files_task = PythonOperator(
        task_id="check_new_files",
        python_callable=check_new_files,
    )

    glue_job_task = GlueJobOperator(
        task_id="trigger_customer_churn_glue_job",
        job_name=GLUE_JOB_NAME,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        script_args={
            "--SOURCE_DATABASE": GLUE_DATABASE,
            "--SOURCE_TABLE": "cust_churn_raw",
            "--OUTPUT_PATH": f"s3://{PROCESSED_BUCKET_NAME}/cleaned_data/",
        },
    )

    athena_refresh_task = AthenaOperator(
        task_id="refresh_athena_table",
        query=f"MSCK REPAIR TABLE {GLUE_DATABASE}.{ATHENA_TABLE};",
        database=GLUE_DATABASE,
        output_location=f"s3://{ATHENA_RESULTS_BUCKET_NAME}/athena-results/",
        aws_conn_id="aws_default",
    )

    check_files_task >> glue_job_task >> athena_refresh_task
