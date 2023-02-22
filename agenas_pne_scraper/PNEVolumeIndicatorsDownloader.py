from bs4 import Tag
import pandas as pd
import re
from typing import Any
from urllib.parse import parse_qs

from .PNEGraphsDownloader import PNEGraphsDownloader
from .PNETableDownloader import PNETableDownloader


class PNEVolumeGraphsDownloader(PNEGraphsDownloader):
    relative_url = 'strutture/grafico1Str5_HC_json.php'
    table_columns = ['anno', 'dati']
    results_columns = [
        'hospital_code',
        'indicator_id',
        'indicator_type',
        'year',
        'value',
    ]

    def generate_querystring_dict(self, **kwargs) -> dict[str, str]:
        return dict(cod_struttura=self.hospital_code, ind=self.indicator_id, **kwargs)

    def process_results_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns={'anno': 'year', 'dati': 'value'})
        df['indicator_type'] = 'volume'
        return df


class PNEVolumeIndicatorsDownloader(PNETableDownloader):
    results_columns = [
        'hospital_code',
        'year',
        'indicator_id',
        'indicator_type',
        'description',
        'value',
    ]
    table_columns = [
        'description',
        'value',
        'indicator_id',
        'operator',
    ]
    relative_url = 'strutture/stru_frequenza.php'

    def generate_querystring_dict(self) -> dict[str, str]:
        return dict(cod_struttura=self.hospital_code)

    def transform_td(self, td: Tag, index: int) -> Any:
        if index == 0:
            return td.text.strip() or None
        elif index == 1:
            try:
                return int(td.text.strip())
            except ValueError:
                pass
        elif index == 2:
            a = td.find('a')
            if a is not None and 'href' in a.attrs:
                qs = parse_qs(re.sub('(.*)\?', '', a.attrs['href']))
                try:
                    return int(qs.get('ind', [''])[0])
                except ValueError:
                    pass

    def process_results_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df['indicator_type'] = 'volume'
        return df.drop(['operator'], axis=1)
