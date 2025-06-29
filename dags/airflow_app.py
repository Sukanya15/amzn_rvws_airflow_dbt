from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import task

def print_hello():
    print("Hello World <<<>>><<<>>>{}{}{}!")

with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2025, 6, 26),
    schedule_interval=None, # Manual trigger
    tags=['example']
) as dag:
    print_task = PythonOperator(
        task_id="print_hello_task",
        python_callable=print_hello,
    )