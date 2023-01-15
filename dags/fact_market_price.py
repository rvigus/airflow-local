from datetime import datetime
import os

from airflow import DAG
from airflow.decorators import task
from operators.GetMarketPricesTimeSeriesOperator import GetMarketPricesTimeSeriesOperator
from utils.constants import ASSETS_DIR
from mixins.common_db_operations_mixin import CommonDBOperationsMixin
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow import XComArg
from utils.common import read_sql

DAG_NAME = 'fact_market_price'
SQL_DIR = os.path.join(ASSETS_DIR, DAG_NAME)
ENV = os.environ.get('environment', 'local')

def get_tickers():
    """
    Fetch list of tickers from ticker dimension

    :return: list
    """

    hook = PostgresHook(f'db_dwh_{ENV}')
    db = CommonDBOperationsMixin()
    df = db.get_records_as_df(
        hook=hook,
        query="select ticker from mart.dim_ticker where ticker is not NULL"
    )

    return df['ticker'].tolist()


with DAG(
    dag_id=DAG_NAME,
    start_date=datetime(2021, 11, 17),
    schedule_interval="45 0 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1
) as dag:

    get_tickers_task = PythonOperator(
        task_id='get_tickers',
        python_callable=get_tickers
    )

    # prices to src
    get_prices = GetMarketPricesTimeSeriesOperator.partial(
        base_url='https://www.alphavantage.co/query?function=TIME_SERIES_DAILY',
        target='market_prices',
        schema='src',
        postgres_url_var=f'dwh_conn_url_{ENV}',
        api_key_var_name='alpha_vantage_api_key',
        task_id='get_prices'
    ).expand(ticker=XComArg(get_tickers_task))

    # src to mart
    delete_from_mart = PostgresOperator(
        postgres_conn_id=f"db_dwh_{ENV}",
        sql=read_sql(os.path.join(SQL_DIR, "delete.sql")),
        task_id="delete_from_mart"
    )

    # insert
    insert_to_mart = PostgresOperator(
        postgres_conn_id=f"db_dwh_{ENV}",
        sql=read_sql(os.path.join(SQL_DIR, "insert.sql")),
        task_id="insert_to_mart"
    )

get_tickers_task >> get_prices >> delete_from_mart >> insert_to_mart
