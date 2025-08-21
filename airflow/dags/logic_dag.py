import sys
from airflow import DAG
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


AIRFLOW_PROJECT_ROOT = '/opt/airflow'
if AIRFLOW_PROJECT_ROOT not in sys.path:
    sys.path.append(AIRFLOW_PROJECT_ROOT)

from src.matching import main as matching_main


def _run_matching_script(**kwargs):
    kwargs['ti'].log.info(f"Running matching_main for DAG run {kwargs['dag_run'].run_id} at {datetime.now()}")
    matching_main()
    kwargs['ti'].log.info(f"matching_main complete.")


with DAG (
    dag_id='fuzzy_matching_silver_tables',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['nyc', 'audit', 'payroll', 'salary', 'gold', 'api','minio']
) as dag:
    
        match_silver_tables_for_business = PythonOperator(
             task_id='match_silver_tables_for_business',
             python_callable=_run_matching_script,
             provide_context=True
        )


match_silver_tables_for_business