from bs4 import Tag
from fake_useragent import UserAgent
import numpy as np
import pandas as pd
import re
import requests
from typing import Any, Callable
from urllib.parse import parse_qs

from .exceptions import ErrorStatusCodeException
from .utils import display, retry
from .PNEGraphsDownloader import PNEGraphsDownloader
from .PNETableDownloader import PNETableDownloader


class PNEOutcomeGraphsDownloader(PNEGraphsDownloader):
    relative_url = 'strutture/grafico1Str1_HC_json.php'
    relative_url_ci = 'strutture/grafico1Str1_IC_HC_json.php'

    table_columns = [
        'anno',
        'dati',
        'ci95_lower',
        'ci95_lower',
    ]

    results_columns = [
        'hospital_code',
        'indicator_id',
        'indicator_type',
        'year',
        'value',
        'ci95_lower',
        'ci95_upper',
    ]

    def generate_querystring_dict(self, **kwargs) -> dict[str, str]:
        return dict(cod_struttura=self.hospital_code, ind=self.indicator_id, **kwargs)

    def process_results_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns={'anno': 'year', 'dati': 'value'})
        df['indicator_type'] = 'outcome'
        return df

    @retry(ErrorStatusCodeException, 10, 1)
    def _request_ci(self, **kwargs) -> requests.Response:
        try:
            r = requests.get(
                self.BASE_URL + self.relative_url_ci,
                headers={'User-Agent': UserAgent()['chrome']},
                params=self.generate_querystring_dict(**kwargs),
            )
        except ConnectionError as e:
            raise ErrorStatusCodeException()
        if r.status_code != 200:
            raise ErrorStatusCodeException(r)
        return r

    def _convert_response_to_df(
        self, r: requests.Response, r_ci: requests.Response
    ) -> pd.DataFrame:
        df = self._adapt_json_to_df(r.json())
        df[['ci95_upper', 'ci95_lower']] = pd.DataFrame(r_ci.json())
        return df

    def download(self, **kwargs) -> pd.DataFrame:
        try:
            r = self._request(**kwargs)
            r_ci = self._request_ci(**kwargs)
            df = self._convert_response_to_df(r, r_ci)
            df = self._parse_response_df(df)
            return df
        except ErrorStatusCodeException as e:
            status_code = e.r.status_code if e.r is not None else None
            print(
                f'hospital_id: {self.hospital_code}, indicator_id: {self.indicator_id} --> r.status_code: {status_code}.'
            )
            return pd.DataFrame([], columns=self.table_columns)


_reg_columns_renamer = {
    'hospital_code': 'hospital_code',
    'year': 'year',
    'indicator_id': 'indicator_id',
    'description': 'description',
    'population': 'population',
    'pct_value': 'pct_value',
    'adj_pct_value': 'adj_pct_value',
    'adj_RR': 'reg_adj_RR',
    'p_value': 'reg_p_value',
}

_prec_columns_renamer = {
    'hospital_code': 'hospital_code',
    'year': 'year',
    'indicator_id': 'indicator_id',
    'description': 'description',
    'population': 'population',
    'pct_value': 'prec_pct_value',
    'adj_pct_value': 'prec_adj_pct_value',
    'adj_RR': 'prec_adj_RR',
    'p_value': 'prec_p_value',
}

_results_columns = [
    'hospital_code',
    'year',
    'indicator_id',
    'indicator_type',
    'description',
    'population',
    'cases',
    'pct_value',
    'adj_pct_value',
    'adj_RR',
    'p_value',
    'reg_adj_RR',
    'reg_p_value',
    'prec_adj_RR',
    'prec_p_value',
]


class PNEOutcomeIndicatorsDownloader(PNETableDownloader):
    reg_columns_renamer = _reg_columns_renamer

    prec_columns_renamer = _prec_columns_renamer

    results_columns = _results_columns

    table_columns = [
        'description',
        'population',
        'pct_value',
        'adj_pct_value',
        'adj_RR',
        'p_value',
        'indicator_id',
        'operator',
    ]

    relative_url = 'strutture/stru_indicatori.php'

    def generate_querystring_dict(self, compare: str) -> dict[str, str]:
        # compare must be "reg" for comparison with regional benchmark or prec for comparison with previous year
        assert compare in set(['reg', 'prec'])
        return dict(cod_struttura=self.hospital_code, conf=compare)

    def transform_td(self, td: Tag, index: int) -> Any:
        if index == 0:
            return td.text.strip() or None
        elif index == 1:
            try:
                return int(td.text.strip())
            except ValueError:
                pass
        elif index in set([2, 3, 4, 5]):
            try:
                return np.float64(td.text.strip())
            except ValueError:
                pass
        elif index == 6:
            a = td.find('a')
            if a is not None and 'href' in a.attrs:
                qs = parse_qs(re.sub('(.*)\?', '', a.attrs['href']))
                try:
                    return int(qs.get('ind', [''])[0])
                except ValueError:
                    pass

    def process_ultimate_results_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df['cases'] = (df.pct_value / 100 * df.population).round(0)
        df['indicator_type'] = 'outcome'
        return df

    def process_results_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop(['operator'], axis=1)
        try:
            df.population = df.population.astype(int)
            # At this point df is not compare-oriented, columns are equals in both compare settings
            return df.copy()
        except Exception:
            display(df)
            print('error while processing volume df')
            return

    @staticmethod
    def rename_problematic_indicators(df: pd.DataFrame) -> pd.DataFrame:
        # indicator description of indicatorwith id 556 is equal to 555, so 555 will be <description> + (v1) and 555 will be <description> + (v2)
        df.loc[
            df.indicator_id == 555, 'description'
        ] = "Proporzione di interventi per tumore maligno della mammella eseguiti in reparti con volume di attivita' superiore a 135 interventi annui (v1)"
        df.loc[
            df.indicator_id == 556, 'description'
        ] = "Proporzione di interventi per tumore maligno della mammella eseguiti in reparti con volume di attivita' superiore a 135 interventi annui (v2)"

        # indicator description of indicatorwith id 999 is equal to 998, so 998 will be <description> + (v1) and 999 will be <description> + (v2)
        df.loc[
            df.indicator_id == 998, 'description'
        ] = "Proporzione di colecistectomie eseguite in reparti con volume di attivita' superiore a 90 interventi annui (v1)"
        df.loc[
            df.indicator_id == 999, 'description'
        ] = "Proporzione di colecistectomie eseguite in reparti con volume di attivita' superiore a 90 interventi annui (v2)"

        return df

    def download(self, compare: str = 'both', **kwargs) -> pd.DataFrame:
        assert compare in set(['both', 'reg', 'prec'])
        if compare == 'both' or compare == 'reg':
            reg_df = super().download(compare='reg')
            # returned cols: 'description', 'value', 'pct_value', 'adj_pct_value', 'adj_RR', 'p_value', 'indicator_id', 'year', 'hospital_code'
            reg_df = reg_df.rename(columns=self.reg_columns_renamer)
            # changed columns names
            reg_df = self.rename_problematic_indicators(reg_df)

        if compare == 'both' or compare == 'prec':
            prec_df = super().download(
                compare='prec'
            )  # remember that this calls also self.process_results_df
            # returned cols: 'description', 'value', 'pct_value', 'adj_pct_value', 'adj_RR', 'p_value', 'indicator_id', 'year', 'hospital_code'
            prec_df = prec_df.rename(columns=self.prec_columns_renamer)
            # changed columns names
            prec_df = self.rename_problematic_indicators(prec_df)

        if compare == 'both':
            if prec_df is None:
                prec_df = pd.DataFrame([], columns=self.reg_columns_renamer)
            prec_df = prec_df.rename(columns={'indicator_id': 'indicator_id_prec'})
            prec_df = prec_df.drop(
                ['population', 'prec_pct_value', 'prec_adj_pct_value'], axis=1
            )

            df = pd.merge(
                reg_df,
                prec_df,
                how='outer',
                left_on=['year', 'hospital_code', 'description'],
                right_on=['year', 'hospital_code', 'description'],
            )
            df.indicator_id = df.indicator_id.fillna(df.indicator_id_prec)
            df = df.drop(['indicator_id_prec'], axis=1)
        elif compare == 'reg':
            df = reg_df.copy()
            # rename not needed as value, pct_value and adj_pct_value have no prefix like in prec_df
        else:
            df = prec_df.copy()
            df = df.rename(
                columns={
                    'population': 'population',
                    'prec_pct_value': 'pct_value',
                    'prec_adj_pct_value': 'adj_pct_value',
                },
            )
        df = self.process_ultimate_results_df(df)
        df = self._order_columns_in_result(df)
        return df

    @classmethod
    def mapper(
        cls, year: int, hospital_code: str, compare: str = 'both', **kwargs
    ) -> pd.DataFrame:
        assert compare in set(['both', 'reg', 'prec'])

        return cls(year=year, hospital_code=hospital_code).download(
            compare=compare, **kwargs
        )

    @classmethod
    def generate_pandas_mapper(
        cls, year: int, compare: str = 'both', **kwargs
    ) -> Callable:
        assert compare in set(['both', 'reg', 'prec'])

        def fn(hospital_code: str) -> pd.DataFrame:
            return cls.mapper(
                year=year, hospital_code=hospital_code, compare=compare, **kwargs
            )

        return fn
