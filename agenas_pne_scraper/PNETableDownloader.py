from bs4 import BeautifulSoup, Tag
from fake_useragent import UserAgent
import pandas as pd

import requests
from typing import Callable
from warnings import warn

from .exceptions import ErrorStatusCodeException
from .utils import BaseClass, display, retry


class PNETableDownloader(BaseClass):
    def __init__(self, year: int, hospital_code: str) -> None:
        assert isinstance(year, int)
        self.year = year
        self.hospital_code = hospital_code

    def generate_querystring_dict(self, **kwargs) -> dict[str, str]:
        """
        Generate a dict that will be used to generate request's querystring. Built in method returns {cod_struttura:<self.hospital_code>}.
        """
        warn('Function not overridden, default is {cod_struttura:<self.hospital_code>}')
        return dict(cod_struttura=self.hospital_code, **kwargs)

    def _add_hospital_id_and_year(self, df: pd.DataFrame) -> pd.DataFrame:
        df['hospital_code'] = self.hospital_code
        df['year'] = self.year
        return df

    def _process_row(self, tr: Tag) -> list[str | int | None]:
        tds = tr.find_all('td')
        if tds is None:
            return
        return map(self.transform_td, list(tds), range(len(tds)))

    def _order_columns_in_result(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Try to order df columns according to self.results_columns. If a column of the list is not found in df, it's simply ignored.
        """
        ret_cols = [col for col in self.results_columns if col in df.columns]
        return df[ret_cols].copy()

    def _parse_request_response(self, r) -> pd.DataFrame:
        bs = BeautifulSoup(r.content, 'lxml')
        data = list(map(self._process_row, bs.find('table').find_all('tr')))
        df = pd.DataFrame(
            data,
            columns=self.table_columns,
        )
        df = df.dropna(axis=0, how='all')
        df = self._add_hospital_id_and_year(df)
        df = self.process_results_df(df)
        df = self._order_columns_in_result(df)
        return df

    @retry(ErrorStatusCodeException, 10, 1)
    def _request(self, **kwargs) -> requests.Response:
        try:
            r = requests.get(
                self.BASE_URL + self.relative_url,
                headers={'User-Agent': UserAgent()['chrome']},
                params=self.generate_querystring_dict(**kwargs),
            )
        except ConnectionError as e:
            raise ErrorStatusCodeException()
        if r.status_code != 200:
            raise ErrorStatusCodeException(r)
        return r

    def download(self, **kwargs) -> pd.DataFrame:
        """
        Make the request, get the response and send it to self._parse_request_response.
        """
        try:
            r = self._request(**kwargs)
        except ErrorStatusCodeException as e:
            status_code = e.r.status_code if e.r is not None else None
            print(
                f'hospital_id: {self.hospital_code}, year: {self.year} --> r.status_code: {status_code}.'
            )
            return pd.DataFrame([], columns=self.table_columns)
        return self._parse_request_response(r)

    @classmethod
    def mapper(cls, year: int, hospital_code: str, **kwargs) -> pd.DataFrame:
        return cls(year=year, hospital_code=hospital_code).download(**kwargs)

    @classmethod
    def generate_pandas_mapper(cls, year: int, **kwargs) -> Callable:
        def fn(hospital_code: str) -> pd.DataFrame:
            return cls.mapper(year=year, hospital_code=hospital_code, **kwargs)

        return fn
