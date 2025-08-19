import os
import sys
from airflow import DAG
from dotenv import load_dotenv
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

load_dotenv()

DBT_PROJECT_PATH = os.getenv("DBT_PROJECT_PATH")

with DAG(
    dag_id="silver_cleaning_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['dbt', 'silver', 'cleaning', 'audit'],
) as dag:

    silver_data = BashOperator(
        task_id='run_dbt_silver',
        bash_command=f'cd {DBT_PROJECT_PATH} && dbt run --select silver',
    )

    silver_data