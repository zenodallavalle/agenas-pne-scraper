from fake_useragent import UserAgent
import pandas as pd

import requests
from typing import Callable
from warnings import warn

from .exceptions import ErrorStatusCodeException
from .utils import BaseClass, retry


class PNEGraphsDownloader(BaseClass):
    def __init__(self, hospital_code: str, indicator_id: str | int | float) -> None:
        assert isinstance(indicator_id, (str, int, float))
        self.hospital_code = hospital_code
        self.indicator_id = (
            indicator_id
            if isinstance(indicator_id, int)
            else int(float(indicator_id))
            if isinstance(indicator_id, str)
            else int(indicator_id)
        )

    def generate_querystring_dict(self, **kwargs) -> dict[str, str]:
        """
        Generate a dict that will be used to generate request's querystring. Built in method returns {cod_struttura:<self.hospital_code>, ind=<self.indicator_id>}.
        """
        warn(
            'Function not overridden, default is {cod_struttura:<self.hospital_code>, ind=<self.indicator_id>}'
        )
        return dict(cod_struttura=self.hospital_code, ind=self.indicator_id, **kwargs)

    def _add_hospital_id_and_indicator_id(self, df: pd.DataFrame) -> pd.DataFrame:
        df['hospital_code'] = self.hospital_code
        df['indicator_id'] = self.indicator_id
        return df

    def _order_columns_in_result(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Try to order df columns according to self.results_columns. If a column of the list is not found in df, it's simply ignored.
        """
        ret_cols = [col for col in self.results_columns if col in df.columns]
        return df[ret_cols].copy()

    @staticmethod
    def _adapt_json_to_df(json_data: list | dict) -> pd.DataFrame:
        def fn(json_element) -> pd.Series:
            return pd.Series(
                json_element['data'], name=str(json_element['name']).lower()
            )

        return pd.concat(map(fn, json_data), axis=1)

    def _convert_response_to_df(self, r: requests.Response) -> pd.DataFrame:
        df = self._adapt_json_to_df(r.json())
        return df

    def _parse_response_df(self, df: pd.DataFrame) -> pd.DataFrame:
        # Handle errors here
        for c in self.table_columns:
            if c not in df.columns:
                raise AssertionError(f"Column '{c}' not in df.columns")

        df = df.dropna(axis=0, how='all')
        df = self._add_hospital_id_and_indicator_id(df)
        df = self.process_results_df(df)
        df = self._order_columns_in_result(df)
        return df

    @retry(ErrorStatusCodeException, 10, 0.2)
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
            df = self._convert_response_to_df(r)
            df = self._parse_response_df(df)
            return df
        except ErrorStatusCodeException as e:
            status_code = e.r.status_code if e.r is not None else None
            print(
                f'hospital_id: {self.hospital_code}, indicator_id: {self.indicator_id} --> r.status_code: {status_code}.'
            )
            return pd.DataFrame([], columns=self.table_columns)

    @classmethod
    def mapper(
        cls, hospital_code: str, indicator_id: str | int | float, **kwargs
    ) -> pd.DataFrame:
        assert isinstance(indicator_id, (str, int, float))
        return cls(hospital_code=hospital_code, indicator_id=indicator_id).download(
            **kwargs
        )

    @classmethod
    def generate_pandas_mapper(cls, **kwargs) -> Callable:
        def fn(row: pd.Series) -> pd.DataFrame:
            assert isinstance(row, pd.Series)
            hospital_code = row.hospital_code
            indicator_id = row.indicator_id
            return cls.mapper(
                hospital_code=hospital_code, indicator_id=indicator_id, **kwargs
            )

        return fn
