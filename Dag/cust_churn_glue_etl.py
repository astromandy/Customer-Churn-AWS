from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta

default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

#  Step 1: Python function to check for new files in S3
def check_new_files():
    s3 = S3Hook(aws_conn_id='aws_default')
    files = s3.list_keys(bucket_name='cust-churn-bucket')
    if not files:
        raise ValueError("No new files in cust-churn-bucket")

#  Step 2: Define DAG at top level
dag = DAG(
    dag_id='churn_etl_to_athena',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='@daily',  # <-- updated parameter name
    catchup=False,
    tags=['etl', 'churn']
)

#  Step 3: Declare tasks inside DAG context
with dag:

    check_files_task = PythonOperator(
        task_id='check_new_files',
        python_callable=check_new_files,
    )

    glue_job_task = GlueJobOperator(
        task_id='trigger_cust_churn_glue_etl2',
        job_name='glue_autoetl',  # Replace with your actual Glue job name
        aws_conn_id='aws_default',
        region_name='us-north-1'   
    )

    athena_refresh_task = AthenaOperator(
        task_id='refresh_athena_table',
        query="MSCK REPAIR TABLE churn_db.cleaned_churn_data;",
        database='churn_db',
        output_location='s3://query-s3result-bucket/cleaned_data/',
        aws_conn_id='aws_default',
    )

    # Step 4: Define task dependencies
    check_files_task >> glue_job_task >> athena_refresh_task
