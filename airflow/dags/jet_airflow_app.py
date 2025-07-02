from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG
data_pipeline_dag = DAG(dag_id='amazon_reviews_data_pipeline',
                         description='End-to-End Data Pipeline: Ingestion, Processing, and DBT Transformation',
                         schedule_interval='0 4 * * *',
                         start_date=datetime(2025, 6, 26),
                         catchup=False,
                         default_args=default_args
                        )

DBT_PROJECT_DIR = '/opt/dbt/dbt_etl'
DBT_PROFILES_DIR = '/opt/airflow/config'

def activate_and_run_dbt(command):
    return (
        f"/opt/venv/bin/dbt {command} "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR}"
    )

# tasks
process_review_data = BashOperator(task_id='process_review_data_for_staging',
                     bash_command='/opt/venv/bin/python3 /opt/airflow/utils/process_and_load_reviews.py',
                     dag=data_pipeline_dag)

process_metadata_category = BashOperator(task_id='process_metadata_category_for_staging',
                     bash_command='/opt/venv/bin/python3 /opt/airflow/utils/process_and_load_metadata.py',
                     dag=data_pipeline_dag)

dbt_snapshot_task = BashOperator(task_id='dbt_run_snapshots',
                     bash_command=activate_and_run_dbt('snapshot'),
                     cwd=DBT_PROJECT_DIR, dag=data_pipeline_dag)

dbt_run_task = BashOperator(task_id='dbt_run_models',
                     bash_command=activate_and_run_dbt('run'),
                     cwd=DBT_PROJECT_DIR, dag=data_pipeline_dag)

dbt_test_task = BashOperator(task_id='dbt_run_tests',
                     bash_command=activate_and_run_dbt('test'),
                     cwd=DBT_PROJECT_DIR, dag=data_pipeline_dag)

#ordering tasks
[process_review_data, process_metadata_category] >> dbt_run_task

dbt_run_task >> dbt_snapshot_task

dbt_snapshot_task >> dbt_test_task

# load_raw_data >> [process_review_data, process_metadata_category]