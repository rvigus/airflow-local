import os
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import json

from airflow.models import BaseOperator, Variable
from utils.common import get_postgres_engine


class GetMarketPricesTimeSeriesOperator(BaseOperator):
    """
    Fetch market prices time series.

    """

    def __init__(
        self,
        base_url,
        ticker,
        target,
        schema,
        postgres_url_var,
        api_key_var_name,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.base_url = base_url
        self.ticker = ticker
        self.target = target
        self.schema = schema
        self.postgres_url_var = postgres_url_var
        self.api_key = Variable.get(api_key_var_name)

    def _create_url(self):
        return self.base_url + f'&symbol={self.ticker}' + f'&apikey={self.api_key}'

    def call_api(self):
        self.log.info(f"Calling api for {self.ticker}")
        r = requests.get(self._create_url())
        self.log.info(r.status_code)
        return r.json()

    def response_to_df(self, response):
        df = pd.DataFrame.from_dict(response['Time Series (Daily)'], orient='index')
        df.columns = [c[2:].strip() for c in df.columns]

        df_meta = pd.DataFrame.from_dict(response['Meta Data'], orient='index').T

        df['symbol'] = df_meta['2. Symbol'].iat[0]
        df['timezone'] = df_meta['5. Time Zone'].iat[0]

        return df

    def execute(self, context):
        try:
            response = self.call_api()
            self.log.info
            df = self.response_to_df(response)
            df.to_sql(name=self.target, schema=self.schema, con=get_postgres_engine(self.postgres_url_var), if_exists='append')
        except:
            self.log.info(f"Failed for {self.ticker}")








