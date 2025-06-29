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

# Define the DAG
data_ingestion_dag = DAG(dag_id='amazon_reviews_raw_data_ingestion',
                         description='Data Ingestion DAG from CSV to PostgreSQL DB',
                         schedule_interval='@daily',
                         start_date=datetime(2025,6,26))

# Define the tasks
# task0 = PythonOperator(task_id='Install-dependencies',
#                        python_callable=install_dependencies,
#                        dag=data_ingestion_dag)
task1 = BashOperator(task_id='data_loading_into_raw_table',
                     bash_command='python3 /opt/airflow/utils/load_raw_data.py',
                     dag=data_ingestion_dag)
task2 = BashOperator(task_id='process_review_data',
                     bash_command='python3 /opt/airflow/utils/process_review_data.py',
                     dag=data_ingestion_dag)
task3 = BashOperator(task_id='process_metadata_category',
                     bash_command='python3 /opt/airflow/utils/process_metadata_category.py',
                     dag=data_ingestion_dag)

# task0>>task1>>task2>>task3>>task4

task1>>task2>>task3