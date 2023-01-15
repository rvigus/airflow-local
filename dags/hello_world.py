from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="hello_world",
    start_date=datetime(2021, 11, 17),
    schedule_interval="45 0 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
)


def print_message(message):
    print(message)


t1 = PythonOperator(
    dag=dag,
    task_id="t1",
    python_callable=print_message,
    op_kwargs={"message": "hello world"},
)
