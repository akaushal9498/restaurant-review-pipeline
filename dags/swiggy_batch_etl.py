# --- dags/swiggy_etl_pipeline.py ---
"""Airflow DAG to orchestrate Swiggy data pipeline."""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utils.job_submitter import submit_spark_job
from utils.load import load_parquet_to_redshift

import os

def run_helper_flatten():
    script_path = os.path.abspath("ingestion/helper_flatten.py")
    submit_spark_job(script_path, mode="onprem")

def run_ingest_swiggy():
    script_path = os.path.abspath("ingestion/ingest_swiggy.py")
    submit_spark_job(script_path, mode="onprem")

def load_to_redshift():
    s3_path = "s3://your-bucket-name/data/refined/processed"
    redshift_table = "public.swiggy_reviews"
    redshift_conn_details = {
        "dbname": "dev",
        "user": "awsuser",
        "password": "yourpassword",
        "host": "your-cluster.region.redshift.amazonaws.com",
        "port": 5439,
        "iam_role": "arn:aws:iam::your-account-id:role/your-redshift-role"
    }
    load_parquet_to_redshift(s3_path, redshift_table, redshift_conn_details)

default_args = {
    "owner": "anup",
    "start_date": days_ago(1),
    "retries": 1
}

dag = DAG(
    "swiggy_etl_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

flatten_task = PythonOperator(
    task_id="helper_flatten",
    python_callable=run_helper_flatten,
    dag=dag
)

ingest_task = PythonOperator(
    task_id="ingest_swiggy",
    python_callable=run_ingest_swiggy,
    dag=dag
)

load_task = PythonOperator(
    task_id="load_to_redshift",
    python_callable=load_to_redshift,
    dag=dag
)

flatten_task >> ingest_task >> load_task
