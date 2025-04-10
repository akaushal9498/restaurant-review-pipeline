from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import os

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

def build_command(path):
    return f"python {os.path.abspath(path)}"

with DAG(
    dag_id='restaurant_data_pipeline',
    default_args=DEFAULT_ARGS,
    description='Ingest, transform, and load Swiggy and Amazon review data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['data_pipeline'],
) as dag:

    with TaskGroup("swiggy_pipeline") as swiggy_group:
        ingest_swiggy = BashOperator(
            task_id="ingest_swiggy",
            bash_command=build_command("ingestion/ingest_swiggy.py")
        )

        transform_swiggy = BashOperator(
            task_id="transform_swiggy",
            bash_command=build_command("transformation/clean_swiggy.py")
        )

        load_swiggy = BashOperator(
            task_id="load_swiggy",
            bash_command=f"python warehouse_loader/load_to_redshift.py swiggy"
        )

        ingest_swiggy >> transform_swiggy >> load_swiggy

    with TaskGroup("amazon_pipeline") as amazon_group:
        ingest_amazon = BashOperator(
            task_id="ingest_amazon",
            bash_command=build_command("ingestion/ingest_amazon_stream.py")
        )

        transform_amazon = BashOperator(
            task_id="transform_amazon",
            bash_command=build_command("transformation/clean_amazon.py")
        )

        load_amazon = BashOperator(
            task_id="load_amazon",
            bash_command=f"python warehouse_loader/load_to_redshift.py amazon"
        )

        ingest_amazon >> transform_amazon >> load_amazon

    swiggy_group >> amazon_group
