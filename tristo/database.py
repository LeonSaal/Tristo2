# -*- coding: utf-8 -*-
"""
Created on Mon Mar  7 11:25:34 2022

@author: Leon
"""
import os
import pandas as pd
from sqlalchemy import create_engine, inspect
from selenium import webdriver
from .complements import SERVICE
from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import datetime


class WVG(SQLModel, table=True):
    id: str = Field(default=None, primary_key=True)
    name: str
    supplied: Optional[int]
    discharge: Optional[float]


class WVG_LAU(SQLModel, table=True):
    LAU: int = Field(default=None, primary_key=True)
    id: str = Field(default=None, foreign_key="wvg.id")
    NUTS: str
    name: str
    full_name: str
    population: Optional[int]
    area_m2: Optional[int]


class Response(SQLModel, table=True):
    LAU: Optional[int] = Field(default=None, primary_key=True)
    position: int
    link: str
    b_href: str
    time: datetime
    status: str
    hash: str
    supplier: Optional[str]
    postcode: Optional[str]
    districts: Optional[str]
    tables: Optional[int]
    pdf: Optional[int]
    err_pdf: Optional[int]
    other: Optional[int]
    err_other: Optional[int]
    years: Optional[str]
    init_link: Optional[str]
    clicks: Optional[int]


class File_Index(SQLModel, table=True):
    hash: str = Field(default=None, foreign_key="response.hash")
    fname: str
    ext: str
    hash2: str = Field(default=None, primary_key=True)


class File_Info(SQLModel, table=True):
    hash2: str = Field(default=None, primary_key=True, foreign_key="file_index.hash2")
    status: Optional[str]
    MB: float
    pages: Optional[int]
    date: Optional[datetime]
    date_orig: Optional[str]
    n_param: Optional[int]
    districts: Optional[str]
    LAUS: Optional[str]
    data_basis: Optional[str]
    analysis: Optional[str]
    OMP: Optional[str]


class File_Cleaned(SQLModel, table=True):
    hash2: str = Field(default=None, primary_key=True, foreign_key="file_index.hash2")
    converter: Optional[str]
    conversion_rate: Optional[str]
    n_params: Optional[int]
    status: Optional[str]
    tabs_total: Optional[int]
    tabs_dropped: Optional[int]
    method: bool = Field(default=False)
    legal_lim: bool = Field(default=False)


class Data(SQLModel, table=True):
    hash2: str = Field(default=None, primary_key=True, foreign_key="file_index.hash2")
    param: str
    unit: Optional[str]
    val: float
    col: int


class Supplier(SQLModel, table=True):
    name: str = Field(default=None, primary_key=True)
    URL: str


TABLES = {
    "ind": "file_index",
    "clean": "cleaned",
    "scrape": "index_scrape",
    "extr": "extracted_tables",
    "data": "dat",
    "mapping": "mapping",
    "info": "file_info",
    "wvg_lau": "WVG_LAU",
    "wvg_inf": "WVG_info",
}

T = pd.DataFrame.from_dict(TABLES, orient="index").squeeze()


def get_from_hash(hash1, sel):
    db = open_db()
    w = f'i.hash="{hash1}"'
    on0 = "c.comm=l.community"
    on1 = "l.LAU=i.LAU"
    sql = f"SELECT DISTINCT {sel} FROM comm_distr c JOIN LAU_comm l ON {on0} JOIN index_scrape i ON {on1} WHERE {w}"
    return pd.read_sql(sql, db.conn)


class open_db:
    def __init__(self):
        (database,) = [file for file in os.listdir() if file.endswith(".db")]
        self.engine = create_engine(f"sqlite:///{database}")
        self.conn = self.engine.connect()

    @property
    def tables(self):
        return inspect(self.engine).get_table_names()

    def read(self, sql):
        return pd.read_sql(sql, self.conn)

    def exe(self, sql):
        self.conn.execute(sql)

    def update(self, *, table, col, new, where):
        old = f"SELECT DISTINCT {col} old, {new} new FROM {table} WHERE {where}"
        print(self.read(old))
        confirmation = ""
        while confirmation not in ["Y", "N"]:
            confirmation = input("Apply changes? [Y/N]")
        if confirmation == "Y":
            new = f"UPDATE {table} SET {col}={new} WHERE {where}"
            self.exe(new)
            print("Successfully updated!")

    def delete(self, *, table, col, where):
        old = f"SELECT DISTINCT {col} FROM {table} WHERE {where}"
        print(self.read(old))
        confirmation = ""
        while confirmation not in ["Y", "N"]:
            confirmation = input("Delete rows? [Y/N]")
        if confirmation == "Y":
            new = f"DELETE FROM {table} WHERE {where}"
            self.exe(new)
            print("Successfully deleteted!")


def get_vals(q: str):
    db = open_db()
    sql = f'SELECT DISTINCT LAU, m.comm, param, unit, val \
        FROM dat d \
        LEFT JOIN mapping m \
        ON d.hash2=m.hash2 \
        LEFT JOIN LAU_comm l \
        ON l.comm=m.comm \
        WHERE param LIKE "{q}" \
        AND LAU IS NOT NULL'
    vals = db.read(sql).astype({"val": float}, errors="ignore")
    if vals.empty:
        return
    vals["val"] = vals["val"].apply(lambda x: pd.to_numeric(x, errors="coerce"))

    unit = vals.unit.value_counts().idxmax()
    param = vals.param.value_counts().idxmax()
    return (
        param,
        unit,
        vals[vals.unit == unit].drop(["param", "unit"], axis=1).dropna(),
    )


def open_file(query: str, hash2: str, q: str = "orig") -> None:
    os.chdir(query)
    if q == "clean":
        path = os.path.join("cleaned", f"{hash2}.xlsx")
        try:
            os.startfile(path)
        except:
            print(f"unable to open {path}")
            os.chdir("..")
            return
    elif q == "conv":
        path = os.path.join("converted", f"{hash2}.xlsx")
        try:
            os.startfile(path)
        except:
            print(f"unable to open {path}")
            os.chdir("..")
            return
    elif q == "orig":
        sql = f'SELECT hash, fname, ext FROM {T.ind} WHERE hash2="{hash2}"'

        db = open_db()
        res = db.read(sql)
        if res.empty:
            print(f"{hash2} not in database")
            os.chdir("..")
            return
        elif res.index.size == 1:
            res = res.T.squeeze()
            path = os.path.join(res.hash, f"{res.fname}{res.ext}")
            os.startfile(path)
    elif q == "web":
        sql = f'SELECT DISTINCT link FROM {T.ind} fi JOIN {T.scrape} sc ON fi.hash=sc.hash WHERE hash2="{hash2}"'
        db = open_db()
        res = db.read(sql)
        res = res.T.squeeze()
        driver = webdriver.Chrome(service=SERVICE)
        driver.get(res)
    os.chdir("..")
