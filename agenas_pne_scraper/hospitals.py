from collections import namedtuple
import numpy as np
import os
import pandas as pd
from typing import Optional
from warnings import warn


HospitalURL = namedtuple("HospitalURL", ["mm_yyyy", "url"])

HOSPITAL_SOURCE_WEBPAGE = (
    "https://www.salute.gov.it/portale/documentazione/p6_2_8_1_1.jsp?id=13"
)
HOSPITAL_SOURCES_URL = [
    HospitalURL(
        "04_2022", "https://www.salute.gov.it/imgs/C_17_bancheDati_13_1_0_file.xlsx"
    )
]

hospital_source_excel_columns_mapper = dict(
    [
        ("Anno", "anno"),
        ("Codice Regione", "codice_regione"),
        ("Descrizione Regione", "regione"),
        ("Codice ASL territoriale", "codice_asl_territoriale"),
        ("Denominazione ASL territoriale", "asl_territoriale"),
        ("Codice Azienda", "codice_azienda"),
        ("Codice struttura", "codice_struttura"),
        ("Denominazione struttura", "struttura"),
        ("Partita IVA", "partita_iva"),
        ("Indirizzo", "indirizzo"),
        ("CAP", "cap"),
        ("Comune", "comune"),
        ("Sigla provincia", "sigla_provincia"),
        ("Codice Comune", "codice_comune"),
        ("Telefono", "telefono"),
        ("Sito web", "sito_web"),
        ("E-mail", "e_mail"),
        ("Codice Tipo struttura", "codice_tipo_struttura"),
        ("Descrizione tipo struttura", "tipo_struttura"),
        ("Giorno apertura", "giorno_apertura"),
        ("Mese apertura", "mese_apertura"),
        ("Anno apertura", "anno_apertura"),
        ("Subcodice struttura interna", "subcodice_struttura_interna"),
        ("Denominazione struttura interna", "struttura_interna"),
        ("Partita IVA struttura interna", "partita_iva_struttura_interna"),
        ("Indirizzo struttura interna", "indirizzo_struttura_interna"),
        ("CAP struttura interna", "cap_struttura_interna"),
        ("Comune struttura interna", "comune_struttura_interna"),
        ("Sigla provincia struttura interna", "sigla_provincia_struttura_interna"),
        ("Codice Comune struttura interna", "codice_comune_struttura_interna"),
        ("Telefono struttura interna", "telefono_struttura_interna"),
        ("Sito web struttura interna", "sito_web_struttura_interna"),
        ("E-mail struttura interna", "email_struttura_interna"),
        ("Giorno apertura struttura interna", "giorno_apertura_struttura_interna"),
        ("Mese apertura struttura interna", "mese_apertura_struttura_interna"),
        ("Anno apertura struttura interna", "anno_apertura_struttura_interna"),
    ]
)

hospitals_df_columns = list(hospital_source_excel_columns_mapper.values()) + [
    "data_up_to"
]

excel_columns_dtypes = dict(
    [
        ("Anno", int),
        ("Codice Regione", str),
        ("Descrizione Regione", str),
        ("Codice ASL territoriale", str),
        ("Denominazione ASL territoriale", str),
        ("Codice Azienda", int),
        ("Codice struttura", str),
        ("Denominazione struttura", str),
        ("Partita IVA", str),
        ("Indirizzo", str),
        ("CAP", str),
        ("Comune", str),
        ("Sigla provincia", str),
        ("Codice Comune", str),
        ("Telefono", str),
        ("Sito web", str),
        ("E-mail", str),
        ("Codice Tipo struttura", str),
        ("Descrizione tipo struttura", str),
        ("Giorno apertura", np.float64),
        ("Mese apertura", np.float64),
        ("Anno apertura", np.float64),
        ("Subcodice struttura interna", str),
        ("Denominazione struttura interna", str),
        ("Partita IVA struttura interna", str),
        ("Indirizzo struttura interna", str),
        ("CAP struttura interna", str),
        ("Comune struttura interna", str),
        ("Sigla provincia struttura interna", str),
        ("Codice Comune struttura interna", str),
        ("Telefono struttura interna", str),
        ("Sito web struttura interna", str),
        ("E-mail struttura interna", str),
        ("Giorno apertura struttura interna", np.float64),
        ("Mese apertura struttura interna", np.float64),
        ("Anno apertura struttura interna", np.float64),
    ]
)

cached_columns_dtypes = dict(
    [
        (k, v)
        for k, v in zip(
            hospital_source_excel_columns_mapper.values(), excel_columns_dtypes.values()
        )
    ]
)
cached_columns_dtypes["codice_regione"] = int

_current_path = os.path.dirname(os.path.abspath(__file__))


def load_hospital_sources_df() -> pd.DataFrame:
    df = pd.DataFrame(HOSPITAL_SOURCES_URL)
    df["mm_yyyy"] = pd.to_datetime(df.mm_yyyy, format="%m_%Y")
    df = df.rename(columns={"mm_yyyy": "data_up_to"})
    df = df.sort_values("data_up_to", ascending=False)
    return df


def _load_cached_df(hospital_sources_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    if "hospitals_cached.csv" in os.listdir(_current_path):
        df = pd.read_csv(
            os.path.join(_current_path, "hospitals_cached.csv"),
            parse_dates=["data_up_to"],
            dtype=cached_columns_dtypes,
        )
        df["data_up_to"] = pd.to_datetime(df.data_up_to, format="%m_%Y")
        if df.data_up_to.min() >= hospital_sources_df.data_up_to.iloc[0]:
            try:
                df[hospitals_df_columns]
                return df
            except KeyError:
                pass


def _download_hospitals_df(
    hospital_sources_df: pd.DataFrame, save_cache: bool = True
) -> pd.DataFrame:
    url = hospital_sources_df.url.iloc[0]
    df = pd.read_excel(url, skiprows=[0], dtype=excel_columns_dtypes)
    df = df.rename(columns=hospital_source_excel_columns_mapper)
    df["codice_regione"] = df.codice_regione.str[0:2].astype(int)
    df["data_up_to"] = hospital_sources_df.data_up_to.iloc[0]
    if save_cache:
        df.to_csv(os.path.join(_current_path, "hospitals_cached.csv"), index=False)
    return df


def get_hospitals_df() -> pd.DataFrame:
    hospital_sources_df = load_hospital_sources_df()
    try:
        hospitals_df = _load_cached_df(hospital_sources_df=hospital_sources_df)
    except Exception as e:
        warn(f'Error while trying to load cached hospitals_df: "{e}"')
        hospitals_df = None
    if hospitals_df is None:
        hospitals_df = _download_hospitals_df(hospital_sources_df=hospital_sources_df)
    return hospitals_df


def get_hospital_id_hospital_name_hospitals_df() -> pd.DataFrame:
    hospitals_df = get_hospitals_df()
    df = pd.concat(
        [
            hospitals_df.codice_struttura.str.cat(
                hospitals_df.subcodice_struttura_interna, na_rep="01", sep=""
            ).to_frame("hospital_id"),
            hospitals_df.struttura_interna.fillna(hospitals_df.struttura).to_frame(
                "hospital_name"
            ),
        ],
        axis=1,
    )
    if df.hospital_id.duplicated().sum() > 0:
        raise AssertionError(
            "There are duplicated values in hospitals_df, correct df before proceeding!"
        )
    return df
