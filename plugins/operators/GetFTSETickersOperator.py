import os
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
from time import sleep

from airflow.models import BaseOperator
from utils.common import get_postgres_engine


class GetFTSETickersOperator(BaseOperator):
    """
    Fetch list of current FTSE stock symbols.

    """

    def __init__(
        self,
        url,
        index_name,
        target,
        schema,
        postgres_url_var,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.url = url
        self.index_name = index_name
        self.target = target
        self.schema = schema
        self.postgres_url_var = postgres_url_var

    def make_request(self, url):
        r = requests.get(url)
        return r.text

    def make_soup(self, response):
        soup = BeautifulSoup(response, 'lxml')
        return soup

    def parse_table(self, soup):

        dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        table = soup.find("table")
        rows = table.find_all("tr")

        tickers = []
        companies = []
        sectors = []

        for row in rows[1:]:
            elements = row.text.split("\n")
            tickers.append(elements[1].replace('.', ''))
            companies.append(elements[2])
            sectors.append(elements[3])

        df = pd.DataFrame(
            {
                "tickers": tickers,
                "company": companies,
                "sectors": sectors,
                "dt": dt,
                "indice": self.index_name,
            },
            index=[i for i in range(0, len(tickers))],
        )

        return df

    def execute(self, context):
        response = self.make_request(url=self.url)
        soup = self.make_soup(response)
        df = self.parse_table(soup)
        df.to_sql(name=self.target, schema=self.schema, con=get_postgres_engine(self.postgres_url_var), if_exists='replace')
        sleep(10)








