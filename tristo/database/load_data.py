# -*- coding: utf-8 -*-
"""
Created on Mon Mar  7 11:25:34 2022

@author: Leon
"""
import io
import logging
import os
import zipfile
from functools import partial

import camelot as cm
import pandas as pd
import requests
import wget
from sqlalchemy.orm import Session
from tqdm import tqdm

from ..utils import download_file
from ..config import LOG_FMT
from ..paths import HOME, PATH_SUPP
from .tables import (
    GN250,
    GV3Q,
    LAU_NUTS,
    WVG,
    WVG_LAU,
    Param,
    Regex,
    Response,
    Supplier,
    Unit,
)

logging.basicConfig(level=logging.INFO, format=LOG_FMT, style="{")
logger = logging.getLogger(__name__)

import sys





def add_external_data(session: Session):
    os.chdir(PATH_SUPP)
    funcs = [load_lau_nuts, load_gv3q, load_gn250, load_wvg, load_supplier]
    for func in funcs:
        objs = func()
        session.add_all(objs)

    os.chdir(HOME)


def load_lau_nuts():
    url = "https://ec.europa.eu/eurostat/documents/345175/501971/EU-27-LAU-2021-NUTS-2021.xlsx"
    file_name = download_file(url, 'LAU-NUTS-Data')
    data = pd.read_excel(
        file_name,
        sheet_name="DE",
        usecols=[0, 1, 2, 5, 6],
        names=["NUTS", "LAU", "name", "population", "area_m2"],
    )
    lau_nuts = []
    logger.info(f"Loading {file_name!r}")
    for row in tqdm(data.itertuples(), total=data.index.size,desc='[ INFO  ]'):
        vals = {
            key: item for key, item in zip(row._fields, row) if key not in ["Index"]
        }
        lau_nuts.append(LAU_NUTS(**vals))

    return lau_nuts


def load_gv3q():
    url = "https://www.destatis.de/DE/Themen/Laender-Regionen/Regionales/Gemeindeverzeichnis/Administrativ/Archiv/GVAuszugQ/AuszugGV3QAktuell.xlsx;jsessionid=8E1964DA2DDF5CEA87120D366C7D08EB.live722?__blob=publicationFile"
    file_name = download_file(url,'Community-Data')
    data = pd.read_excel(
        file_name,
        sheet_name="Onlineprodukt_Gemeinden",
        usecols=[2, 3, 4, 5, 6, 7],
        names=["Land", "RB", "Kreis", "VB", "Gem", "name"],
        skiprows=6,
        dtype=str,
        keep_default_na=False,
    )
    data = data[data.Land != ""]
    gv3qs = []
    logger.info(f"Loading {file_name!r}")
    for row in tqdm(data.itertuples(), total=data.index.size,desc='[ INFO  ]'):
        ars = f"{row.Land}{row.RB}{row.Kreis}{row.VB}{row.Gem}"
        gv3qs.append(GV3Q(ars=ars, name=row.name))

    return gv3qs


def load_wvg():
    file = "WVG.xlsx"
    if not os.path.exists(file):
        logger.warn(f"{file!r} does not exist!")
        return []
    data = pd.read_excel(
        file,
    ).dropna()
    wvgs = []
    wvg_laus = []
    logger.info(f"Loading {file!r}")
    for row in tqdm(data.itertuples(), total=data.index.size,desc='[ INFO  ]'):
        vals = {
            key: item
            for key, item in zip(row._fields, row)
            if key not in ["Index", "LAU"]
        }
        wvgs.append(WVG(**vals))

        for LAU in str(row.LAU).split(", "):
            wvg_laus.append(WVG_LAU(wvg=row.Index + 1, LAU=LAU))

    return wvgs + wvg_laus


def load_gn250():
    url = "https://daten.gdz.bkg.bund.de/produkte/sonstige/gn250/aktuell/gn250.gk3.csv.zip"
    file_name = download_file(url, 'GN250-Data')

    with zipfile.ZipFile(file_name) as arch:
        file = arch.read("gn250.gk3.csv/gn250/GN250.csv")

    data = pd.read_csv(
        io.BytesIO(file),
        sep=";",
        header=0,
        usecols=[4, 12],
        names=["name", "ars"],
        nrows=43967,
    )
    gn250s = []
    logger.info(f"Loading {file_name!r}")
    for row in tqdm(data.itertuples(), total=data.index.size,desc='[ INFO  ]'):
        vals = {
            key: item for key, item in zip(row._fields, row) if key not in ["Index"]
        }
        gn250s.append(GN250(**vals))

    return gn250s


def load_supplier():
    file = "Supplier.xlsx"
    if not os.path.exists(file):
        logger.warn(f"{file!r} does not exist!")
        return []
    data = pd.read_excel(file)
    suppliers = []
    logger.info(f"Loading {file!r}")
    for row in tqdm(data.itertuples(), total=data.index.size,desc='[ INFO  ]'):
        vals = {
            key: item for key, item in zip(row._fields, row) if key not in ["Index"]
        }
        suppliers.append(Supplier(**vals))

    return suppliers


def load_table_from_file(table_model):
    file = f"{table_model.__tablename__}.xlsx"
    if not os.path.exists(file):
        logger.warn(f"{file!r} does not exist!")
        return []

    kwargs = {}
    if table_model.__tablename__ == "response":
        kwargs = {"parse_dates": ["time"]}
    data = pd.read_excel(file, **kwargs)
    inst = []
    logger.info(f"Loading {file!r}")
    for row in tqdm(data.itertuples(), total=data.index.size,desc='[ INFO  ]'):
        vals = {
            key: item for key, item in zip(row._fields, row) if key not in ["Index"]
        }
        inst.append(table_model(**vals))

    return inst


def load_tables_from_file(session: Session):
    os.chdir(PATH_SUPP)
    funcs = [
        partial(load_table_from_file, table) for table in [Regex, Unit, Param, Response]
    ]
    for func in funcs:
        objs = func()
        session.add_all(objs)
    os.chdir(HOME)


def add_params(session: Session):
    funcs = [load_TrinkwV, load_uqn, load_psm, load_gow, load_eutwrl]

    limits = []
    for func in funcs:
        limits.append(func())

    data = pd.concat(limits)
    params = []
    for row in tqdm(data.itertuples(), total=data.index.size,desc='[ INFO  ]'):
        vals = {
            key: item
            for key, item in zip(row._fields, row)
            if key not in ["Index", "LAU", "districts", "supplier"]
        }
        params.append(Param(**vals))
    session.add_all(params)


def load_TrinkwV():
    anlagen = []
    for anlage in [1, 2, 3, "3a"]:
        name = f"TrinkwV Anlage {anlage}"
        url = f"https://www.gesetze-im-internet.de/trinkwv_2001/anlage_{anlage}.html"
        logger.info(f"Loading {name} from {url!r}")
        page = requests.get(url)

        if not page.ok:
            print(f"{page.status_code}")
            return

        path = os.path.join(PATH_SUPP, f"{name}.html")
        with open(path, "wb") as file:
            file.write(page.content)

        data = pd.read_html(io.BytesIO(page.content), decimal=",", thousands=".")
        new = pd.DataFrame()
        if anlage == 1:
            table = data[0].iloc[:, [1, 2]]
            new["param"] = table.iloc[:, 0]
            limit = table.iloc[:, 1].str.split("/", expand=True)
            new["limit"], new["unit"] = limit.iloc[:, 0], "Anzahl/" + limit.iloc[:, 1]

        if anlage == 2:
            table = pd.concat(data).iloc[:, [1, 2]]
            new["param"], new["limit"], new["unit"] = (
                table.iloc[:, 0],
                table.iloc[:, 1],
                "mg/l",
            )

        if anlage == 3:
            table = data[0].iloc[:, [1, 2, 3]]
            new["param"], new["limit"], new["unit"] = (
                table.iloc[:, 0],
                table.iloc[:, 2],
                table.iloc[:, 1],
            )

        if anlage == "3a":
            table = data[0].iloc[:, 1:]
            new["param"], new["limit"], new["unit"] = (
                table.iloc[:, 0],
                table.iloc[:, 1],
                table.iloc[:, 2],
            )

        new["origin"] = f"TrinkwV Anlage {anlage}"

        anlagen.append(new)

    return pd.concat(anlagen)


def load_eutwrl():
    url = "https://eur-lex.europa.eu/legal-content/DE/TXT/HTML/?uri=CELEX:32020L2184&from=de"
    logger.info(f"Loading EUWD-Data from {url!r}")
    pat = "Parameterwert"
    data = pd.read_html(url, match=pat, header=0, decimal=",", thousands=".")
    table = pd.concat([table for table in data if pat in table.columns])
    new = pd.DataFrame()

    new["param"], new["limit"], new["unit"] = (
        table.iloc[:, 0],
        table.iloc[:, 1],
        table.iloc[:, 2],
    )
    new["origin"] = "Richtlinie (EU) 2020/2184"
    new.dropna(subset="param", inplace=True)
    return new[new.param != "—"]


def load_uqn():
    url = "https://eur-lex.europa.eu/legal-content/DE/TXT/HTML/?uri=CELEX:32013L0039&from=DE"
    logger.info(f"Loading UQN-Data from {url!r}")
    data = pd.read_html(url, match="Stoffname", skiprows=2, decimal=",", thousands=".")
    table = data[0].iloc[:, [1, 2, 3]].replace(regex={"\s[(]\d+[)]": ""})
    new = pd.DataFrame()
    new["param"], new["limit"], new["unit"], new["CAS"] = (
        table.iloc[:, 0],
        table.iloc[:, 2]
        .replace(regex={",": ".", "\s×\s10–": "e-"})
        .astype(float, errors="ignore"),
        "µg/l",
        table.iloc[:, 1],
    )
    new["origin"] = "Richtlinie 2013/39/EU"
    return new


def load_gow():
    url = "https://www.umweltbundesamt.de/sites/default/files/medien/5620/dokumente/listegowstoffeohnepsm-20200728-homepage_kopie_0.pdf"
    file_name = download_file(url,'GOW-Data')
    data = cm.read_pdf(file_name, pages="all")
    table = pd.concat([table.df for table in data])
    new = pd.DataFrame()
    limit = table[2].str.split(" ", expand=True)
    new["param"], new["limit"], new["unit"], new["category"], new["CAS"] = (
        table[0],
        limit[0].str.replace(",", ".").astype(float),
        limit[1],
        table[4],
        table[1],
    )
    new["origin"] = "GOW"
    return new


def load_psm():
    url = "https://www.bvl.bund.de/SharedDocs/Downloads/04_Pflanzenschutzmittel/psm_wirkstoffe_in_kulturen.zip?__blob=publicationFile&v=17"
    file_name = download_file(url, 'PSM-Data')
    with zipfile.ZipFile(file_name) as arch:
        file = arch.read("WstKulturHistorie-2022-07.xlsx")

    table = pd.read_excel(file, sheet_name=1, usecols=[3, 5])
    table.drop_duplicates(inplace=True)
    new = pd.DataFrame()
    new["param"], new["category"] = table.iloc[:, 0], table.iloc[:, 1]
    new["origin"] = "zugelassene PSM nach Kulturen"
    return new


# def link_supplier():
#     with OpenDB().session() as session:
#         stmt = select(Supplier.url, Supplier.id)
#         for url, id in session.execute(stmt).all():
#             if not url:
#                 continue

#             stmt = select(Response.b_href).where(Response.link.regexp_match(url))
#             for regex in session.execute(stmt).scalars().all():
#                 stmt = (
#                     update(Response).where(Response.b_href == regex).values(supplier=id)
#                 )
#                 session.execute(stmt)
#         session.commit()


# TABLES = {
#     "ind": "file_index",
#     "clean": "cleaned",
#     "scrape": "index_scrape",
#     "extr": "extracted_tables",
#     "data": "dat",
#     "mapping": "mapping",
#     "info": "file_info",
#     "wvg_lau": "WVG_LAU",
#     "wvg_inf": "WVG_info",
# }

# T = pd.DataFrame.from_dict(TABLES, orient="index").squeeze()


# def get_from_hash(hash1, sel):
#     db = open_db()
#     w = f'i.hash="{hash1}"'
#     on0 = "c.comm=l.community"
#     on1 = "l.LAU=i.LAU"
#     sql = f"SELECT DISTINCT {sel} FROM comm_distr c JOIN LAU_comm l ON {on0} JOIN index_scrape i ON {on1} WHERE {w}"
#     return pd.read_sql(sql, db.conn)


# class open_db:
#     def __init__(self):
#         (database,) = [file for file in os.listdir() if file.endswith(".db")]
#         self.engine = create_engine(f"sqlite:///{database}")
#         self.conn = self.engine.connect()

#     @property
#     def tables(self):
#         return inspect(self.engine).get_table_names()

#     def read(self, sql):
#         return pd.read_sql(sql, self.conn)

#     def exe(self, sql):
#         self.conn.execute(sql)

#     def update(self, *, table, col, new, where):
#         old = f"SELECT DISTINCT {col} old, {new} new FROM {table} WHERE {where}"
#         print(self.read(old))
#         confirmation = ""
#         while confirmation not in ["Y", "N"]:
#             confirmation = input("Apply changes? [Y/N]")
#         if confirmation == "Y":
#             new = f"UPDATE {table} SET {col}={new} WHERE {where}"
#             self.exe(new)
#             print("Successfully updated!")

#     def delete(self, *, table, col, where):
#         old = f"SELECT DISTINCT {col} FROM {table} WHERE {where}"
#         print(self.read(old))
#         confirmation = ""
#         while confirmation not in ["Y", "N"]:
#             confirmation = input("Delete rows? [Y/N]")
#         if confirmation == "Y":
#             new = f"DELETE FROM {table} WHERE {where}"
#             self.exe(new)
#             print("Successfully deleteted!")


# def get_vals(q: str):
#     db = open_db()
#     sql = f'SELECT DISTINCT LAU, m.comm, param, unit, val \
#         FROM dat d \
#         LEFT JOIN mapping m \
#         ON d.hash2=m.hash2 \
#         LEFT JOIN LAU_comm l \
#         ON l.comm=m.comm \
#         WHERE param LIKE "{q}" \
#         AND LAU IS NOT NULL'
#     vals = db.read(sql).astype({"val": float}, errors="ignore")
#     if vals.empty:
#         return
#     vals["val"] = vals["val"].apply(lambda x: pd.to_numeric(x, errors="coerce"))

#     unit = vals.unit.value_counts().idxmax()
#     param = vals.param.value_counts().idxmax()
#     return (
#         param,
#         unit,
#         vals[vals.unit == unit].drop(["param", "unit"], axis=1).dropna(),
#     )


# def open_file(query: str, hash2: str, q: str = "orig") -> None:
#     os.chdir(query)
#     if q == "clean":
#         path = os.path.join("cleaned", f"{hash2}.xlsx")
#         try:
#             os.startfile(path)
#         except:
#             print(f"unable to open {path}")
#             os.chdir("..")
#             return
#     elif q == "conv":
#         path = os.path.join("converted", f"{hash2}.xlsx")
#         try:
#             os.startfile(path)
#         except:
#             print(f"unable to open {path}")
#             os.chdir("..")
#             return
#     elif q == "orig":
#         sql = f'SELECT hash, fname, ext FROM {T.ind} WHERE hash2="{hash2}"'

#         db = open_db()
#         res = db.read(sql)
#         if res.empty:
#             print(f"{hash2} not in database")
#             os.chdir("..")
#             return
#         elif res.index.size == 1:
#             res = res.T.squeeze()
#             path = os.path.join(res.hash, f"{res.fname}{res.ext}")
#             os.startfile(path)
#     elif q == "web":
#         sql = f'SELECT DISTINCT link FROM {T.ind} fi JOIN {T.scrape} sc ON fi.hash=sc.hash WHERE hash2="{hash2}"'
#         db = open_db()
#         res = db.read(sql)
#         res = res.T.squeeze()
#         driver = webdriver.Chrome(service=SERVICE)
#         driver.get(res)
#     os.chdir("..")
