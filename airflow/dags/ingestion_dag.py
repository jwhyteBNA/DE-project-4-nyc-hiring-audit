import sys
from airflow import DAG
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


AIRFLOW_PROJECT_ROOT = '/opt/airflow'
if AIRFLOW_PROJECT_ROOT not in sys.path:
    sys.path.append(AIRFLOW_PROJECT_ROOT)

from src.ingest import main as api_ingestion_main
from src.connection import main as raw_data_ingestion_main


def _run_data_ingestion_script(**kwargs):
    kwargs['ti'].log.info(f"Running api_ingestion_main for DAG run {kwargs['dag_run'].run_id} at {datetime.now()}")
    api_ingestion_main()
    kwargs['ti'].log.info(f"api_ingestion_main complete.")

def _run_raw_data_ingestion_script(**kwargs):
    kwargs['ti'].log.info(f"Running raw_data_ingestion_main for DAG run {kwargs['dag_run'].run_id} at {datetime.now()}")
    raw_data_ingestion_main()
    kwargs['ti'].log.info(f"raw_data_ingestion_main complete.")


with DAG (
    dag_id='data_ingestion_pipeline',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['nyc', 'audit', 'payroll', 'salary', 'bronze', 'api','minio']
) as dag:
    
        pull_api_data_to_minio_task = PythonOperator(
             task_id='pull_api_data_to_minio',
             python_callable=_run_data_ingestion_script,
             provide_context=True
        )

        stage_minio_to_snowflake_task = PythonOperator(
             task_id='stage_minio_to_snowflake',
             python_callable=_run_raw_data_ingestion_script,
             provide_context=True
        )


pull_api_data_to_minio_task >> stage_minio_to_snowflake_task