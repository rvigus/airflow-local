from datetime import datetime
import os

from airflow import DAG
from operators.GetFTSETickersOperator import GetFTSETickersOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from utils.constants import ASSETS_DIR
from utils.common import read_sql

DAG_NAME = 'dim_tickers'
SQL_DIR = os.path.join(ASSETS_DIR, DAG_NAME)
ENV = os.environ.get('environment', 'local')

dag = DAG(
    dag_id=DAG_NAME,
    start_date=datetime(2021, 11, 17),
    schedule_interval="45 0 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1
)

tickers_to_src = GetFTSETickersOperator(
    postgres_url_var=f'dwh_conn_url_{ENV}',
    url="https://www.fidelity.co.uk/shares/ftse-100/",
    index_name='ftse100',
    target='ftse_tickers',
    schema='src',
    task_id='tickers_to_src',
    dag=dag
)

src_to_dim = PostgresOperator(
    postgres_conn_id=f'db_dwh_{ENV}',
    sql=read_sql(os.path.join(SQL_DIR, 'src_to_dim.sql'), multi=True),
    task_id='src_to_dim',
    autocommit=True,
    dag=dag
)

tickers_to_src >> src_to_dim

