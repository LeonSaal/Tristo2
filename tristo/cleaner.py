# -*- coding: utf-8 -*-
"""
Created on Thu Feb 10 10:55:46 2022

@author: Leon
"""

from typing import Dict, Mapping, Tuple

import numpy as np
import pandas as pd
from IPython.display import clear_output
from pint import UnitRegistry
from sqlalchemy import String, cast, delete, select, update
from sqlalchemy.orm import Session
from tqdm import tqdm

from .complements import PATS, UNITS
from .database import Data, File_Cleaned, File_Index, File_Info, TableData
from .index_utils import get_col, get_orientation, make_index_col
from .paths import PATH_CONV
from .status import Status
from .utils import count_occ, is_number

ureg = UnitRegistry()
import logging

from .config import LOG_FMT

logging.basicConfig(level=logging.INFO, format=LOG_FMT, style="{")
logger = logging.getLogger(__name__)

def encode_scanned(tables:Dict[str, pd.DataFrame]):
    for num in tables.keys():
        tables[num] = tables[num].applymap(lambda x: str(x).encode())
    return tables


def tables_to_db(session: Session, overwrite=False, **kwargs) -> None:
    if overwrite:
        del_data = delete(Data).where(True)
        del_tabdata = delete(TableData).where(True)
        reset_status = update(File_Cleaned).values(status=None)
        session.execute(del_data)
        session.execute(del_tabdata)
        session.execute(reset_status)
        session.commit()

    statement = (
        select(File_Index.hash2, File_Info.status)
        .outerjoin(File_Info)
        .outerjoin(File_Cleaned)
        .where(
            File_Cleaned.converter != Status.ERROR,
            File_Cleaned.status == None,
        )
    )

    for hash2, status in (
        pbar := tqdm(session.execute(statement).all(), desc="[ INFO  ]")
    ):
        stat = {}
        path_extr = PATH_CONV / f"{hash2}.xlsx"
        pbar.set_postfix_str(hash2)
        tables = pd.read_excel(path_extr, header=0, index_col=0, sheet_name=None)
        if status == Status.SCAN:
            tables = encode_scanned(tables)
        stat["tabs_total"] = len(tables)
        tables = find_tables(tables, **kwargs)
        tables, stats = clean_tables(tables)
        stat["tabs_dropped"] = stat["tabs_total"] - len(tables)
        stat.update(stats)

        data = []
        for num, table in tables.items():
            for col, series in table.items():
                for (param, unit), vals in series.dropna().items():
                    data.append(
                        Data(
                            hash2=hash2,
                            tab=num,
                            col=col,
                            param=param,
                            unit=unit,
                            val=vals,
                        )
                    )
            data.append(TableData(hash2=hash2, tab=num, method=stat[num]["method"]))
        stmt = (
            update(File_Cleaned)
            .where(File_Cleaned.hash2 == hash2)
            .values(
                status=Status.INSERTED if tables else Status.OMITTED,
                tabs_total=stat["tabs_total"],
                tabs_dropped=stat["tabs_dropped"],
                n_params=count_occ(list(tables.values()), PATS["PARAMS"]),
            )
        )
        session.execute(stmt)
        session.add_all(data)
        session.commit()
    cast_vals = update(Data).values(param=cast(Data.param, String), unit = cast(Data.unit, String), val = cast(Data.val, String))
    session.execute(cast_vals)
    session.commit()


def orient_data(df):
    new = df.copy()
    new = new.replace("", np.nan)
    if new.empty:
        return
    _, axis = get_orientation(new, PATS["PARAMS"])
    if axis == 1:
        new = new.T
    return new


def clean_tables(tables: dict) -> Tuple[dict, dict]:
    stats = {}
    out = {}

    for num, table in tables.items():
        stats[num] = {}
        make_unit_col(table)
        make_index_col(table, thresh=0.2)
        if "param" in table and "unit" in table:
            table.set_index(["param", "unit"], inplace=True)
        else:
            continue
        stats[num]["method"] = drop_col(table, PATS["METHOD"], thresh=0.1)
        drop_string_cols(table)

        table.dropna(axis=1, how="all")
        table.dropna(axis=0, how="all")
        table.rename(
            {
                old: new
                for old, new in zip(table.columns, np.arange(table.columns.size))
            },
            axis=1,
            inplace=True,
        )
        out[num] = table
    return out, stats


def drop_col(data: pd.DataFrame, keys, **kwargs) -> bool:
    index = get_col(data, keys, **kwargs)
    if not index.empty:
        data.drop(index, axis=1, inplace=True)
        return True
    else:
        return False


def drop_string_cols(df: pd.DataFrame, thresh: float = 0.75) -> None:
    mask = ~df.applymap(is_number)
    rel_mask = mask.sum() / df.count()
    index_col = get_col(df, PATS["PARAMS"], thresh=0.1)
    index = rel_mask[rel_mask > thresh].index.difference(index_col)
    df.drop(index, axis=1, inplace=True)


def make_unit_col(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    index_col = get_col(df, PATS["PARAMS"], thresh=0.2)
    unit_col = get_col(df, PATS["UNIT"], **kwargs)
    if index_col.size == unit_col.size:
        if index_col.size != 1:
            return df
        else:
            units = df[unit_col]
            if index_col != unit_col:
                df.drop(unit_col, axis=1, inplace=True)
    else:
        if unit_col.size == 1:
            units = df[unit_col]
            df.drop(unit_col, axis=1, inplace=True)

        elif unit_col.size > 1:
            unit_df = (
                df[unit_col]
                .replace(regex=UNITS)
                .apply(lambda x: x.astype(str).str.findall(r"\D+\Z").str.join(""))
            )
            units = unit_df.T.apply(lambda x: x.dropna().value_counts().idxmax())
        else:
            unit_col = get_col(df, PATS["UNIT"], thresh=1)
            if unit_col.size == 1:
                row = get_col(df.T, PATS["UNIT"], thresh=1)
                if row.size == 1:
                    unit = df.loc[row, unit_col]
                    units = pd.Series(np.full(df.index.size, unit))
                else:
                    units = pd.Series(np.full(df.index.size, "?"))
            else:
                units = pd.Series(np.full(df.index.size, "?"))
    units = pd.Series(units.squeeze()).fillna(method="ffill").to_list()
    df.insert(0, "unit", units)
    num_pat = "^(?P<num>[-\d,.\s<>–nbg]+).*$"
    df.replace(regex={num_pat: "\g<num>"}, inplace=True)
    return df


def find_tables(tables: Mapping) -> None:
    to_del = []
    for key, table in tables.items():
        string = table.apply(lambda x: x.astype(str).str.contains(PATS["PARAMS"]))
        if not np.any(string):
            to_del.append(key)
    (tables.pop(key) for key in to_del)
    return tables
