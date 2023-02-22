from bs4 import Tag
import pandas as pd
import re
from typing import Any
from urllib.parse import parse_qs

from .utils import display
from .PNEGraphsDownloader import PNEGraphsDownloader
from .PNETableDownloader import PNETableDownloader


class PNEWaitingTimeGraphsDownloader(PNEGraphsDownloader):
    relative_url = 'strutture/grafico1Str3_HC_json.php'
    table_columns = ['anno', 'dati']
    results_columns = [
        'hospital_code',
        'indicator_id',
        'indicator_type',
        'year',
        'adj_median',
    ]

    def generate_querystring_dict(self, **kwargs) -> dict[str, str]:
        return dict(cod_struttura=self.hospital_code, ind=self.indicator_id, **kwargs)

    def process_results_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns={'anno': 'year', 'dati': 'adj_median'})
        df['indicator_type'] = 'wt'
        return df


class PNEWaitingTimeIndicatorsDownloader(PNETableDownloader):
    results_columns = [
        'hospital_code',
        'year',
        'indicator_id',
        'indicator_type',
        'description',
        'cases',
        'intervention_pct',
        'median',
        'adj_median',
    ]

    table_columns = [
        'description',
        'cases',
        'intervention_pct',
        'median',
        'adj_median',
        'indicator_id',
        'survivals',
    ]

    relative_url = 'strutture/stru_tempi.php'

    def generate_querystring_dict(self) -> dict[str, str]:
        return dict(cod_struttura=self.hospital_code)

    def transform_td(self, td: Tag, index: int) -> Any:
        if index == 0:
            return td.text.strip() or None
        elif index in set([1, 2, 3, 4]):
            try:
                return int(td.text.strip())
            except ValueError:
                pass
        elif index == 5:
            a = td.find('a')
            if a is not None and 'href' in a.attrs:
                qs = parse_qs(re.sub('(.*)\?', '', a.attrs['href']))
                try:
                    return int(qs.get('ind', [''])[0])
                except ValueError:
                    pass

    def process_results_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df['indicator_type'] = 'wt'
        return df.drop(['survivals'], axis=1)
