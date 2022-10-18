# -*- coding: utf-8 -*-
"""
Created on Thu Feb 10 10:55:46 2022

@author: Leon
"""

import re
from typing import Mapping, Tuple

import numpy as np
import pandas as pd
from IPython.display import clear_output
from pint import UnitRegistry
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from .complements import PATS, UNITS, Status
from .database import Data, File_Cleaned, File_Index, File_Info, TableData
from .index_utils import get_col, get_orientation, make_index_col
from .paths import PATH_CONV
from .utils import count_occ, is_number

ureg = UnitRegistry()


def tables_to_db(session: Session, **kwargs) -> None:
    statement = (
        select(File_Index.hash2)
        .outerjoin(File_Info)
        .outerjoin(File_Cleaned)
        .where(
            File_Cleaned.converter != Status.ERROR,
            File_Cleaned.status == None,
            File_Info.status != Status.SCAN,
        )
    )
    for i, hash2 in enumerate(result := session.execute(statement).scalars().all()):
        stat = {}
        path_extr = PATH_CONV / f"{hash2}.xlsx"
        print(f'{i+1} of {len(result)}: "{path_extr}"')
        tables = pd.read_excel(path_extr, header=0, index_col=0, sheet_name=None)
        stat["tabs_total"] = len(tables)
        find_tables(tables, **kwargs)
        tables, stats = clean_tables(tables, session=session)
        stat["tabs_dropped"] = stat["tabs_total"] - len(tables)
        stat.update(stats)

        data = []
        for num, table in tables.items():
            for col, series in table.iteritems():
                for ind, vals in series.iteritems():
                    data.append(
                        Data(
                            hash2=hash2,
                            tab=num,
                            col=col,
                            param=ind[0],
                            unit=ind[1],
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
        clear_output(wait=True)
        session.commit()


def orient_data(df):
    new = df.copy()
    new = new.replace("", np.nan)
    if new.empty:
        return
    _, axis = get_orientation(new, PATS["PARAMS"])
    if axis == 1:
        new = new.T
    # data_cols = new.columns.difference(index)
    # new[data_cols] = new[data_cols].replace(
    #     regex=[",", "-|–", r"<\s*"], value=[".", "--", "-"]
    # )
    return new


def clean_tables(
    tables: dict, session: Session, thresh: float = 0.1, **kwargs
) -> Tuple[dict, dict]:
    stats = {}
    out = {}

    for num, table in tables.items():
        stats[num] = {}
        # table = split_lengthwise(table)
        make_unit_col(table)
        make_index_col(table, thresh=0.2)

        if "param" in table and "unit" in table:
            table.set_index(["param", "unit"], inplace=True)
        elif "param" in table:
            table.set_index(["param"], inplace=True)
        else:
            continue
        # convert_units(table, thresh=0.1)
        stats[num]["method"] = drop_col(table, PATS["METHOD"], thresh=0.1)
        # if "unit" in table.index.names:
        #     stats[num]["legal_lim"] = drop_limit_col(table, session=session)
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
    [tables.pop(key) for key in to_del]


# def expand_duplicate_index(df):
#     index_col = get_col(df, KEYWORDS['params'], thresh=0.1)
#     if index_col.empty:
#         return df
#     else:
#         index_col = index_col[0]
#     index_iloc = df.columns.get_loc(index_col)
#     no_dups = []
#     residue = df.copy()
#     while True:
#         dups = residue.duplicated(subset=index_col)
#         no_dup = residue[~dups]
#         no_dups.append(no_dup)
#         residue = residue[dups]
#         if not residue[index_col].duplicated().any():
#             no_dups.append(residue)
#             break
#     for i, df in enumerate(no_dups):
#         oldies = df.columns.get_level_values(2).to_list()
#         newbs = [f'{old}.{i}' for old in oldies]
#         df.rename({old: new for old, new in zip(oldies, newbs)},
#                   level=2, axis=1, inplace=True)

#     new = no_dups[0].copy()
#     left = [no_dups[0].columns[index_iloc]]
#     for i, df in enumerate(no_dups):
#         if i == 0:
#             continue
#         else:
#             right = [df.columns[index_iloc]]
#             new = new.merge(df, left_on=left, right_on=right, how='outer')
#             new.drop(df.columns[index_iloc], axis=1, inplace=True)
#     return new


# def clean_index(data: pd.DataFrame, **kwargs):
#     col = get_col(data, KEYWORDS["PARAMS"], **kwargs)
#     data[col] = (
#         data[col].fillna(data.index.to_series()).replace(regex=TO_REPLACE["index"])
#     )


# def convert_units(df: pd.DataFrame, **kwargs):
#     if "param" not in df.index.names or "unit" not in df.index.names:
#         return
#     df.reset_index(inplace=True)
#     df.set_index("param", inplace=True)
#     for i, (index, values) in enumerate(df.iterrows()):
#         param = index
#         unit = values["unit"]
#         if param in UNITS and type(unit) == str:
#             default_unit = UNITS[param]
#             factor = compare_units(unit, default_unit, param)
#             if factor:
#                 df.iloc[i] = values.apply(rescale_value, factor=factor)
#                 df.iloc[i, 0] = default_unit
#     df.reset_index(inplace=True)
#     df.set_index(["param", "unit"], inplace=True)


# def compare_units(unit: str, default_unit: str, param: str) -> float or None:
#     factor = 1
#     if not unit and not default_unit:
#         return None

#     if "mol" in unit and param in MOLAR_MASS:
#         unit = re.sub("mol", "g", unit)
#         factor *= MOLAR_MASS[param]

#     try:
#         if (unit in ureg) and (default_unit in ureg):
#             factor *= ureg.Quantity(unit).to(default_unit).magnitude
#             return factor
#     except Exception:
#         return None


# def clean_tables_sql(query, overwrite=False, **kwargs):
#     os.chdir(query)

#     db = open_db()

#     if overwrite and T.clean in db.tables:
#         db.exe(f"DROP TABLE {T.clean}")

#     cols = ["cleaned", "tabs_total", "tabs_dropped", "method", "legal_lim"]
#     if T.clean not in db.tables:
#         pd.DataFrame(columns=cols).to_sql(T.clean, db.conn, index_label="hash2")

#     if T.extr not in db.tables:
#         print("No converted tables!")
#         return

#     if T.mapping not in db.tables:
#         cols_data = ["comm", "distr", "col"]
#         pd.DataFrame(columns=cols_data).to_sql(T.mapping, db.conn, index_label="hash2")

#     if T.data not in db.tables:
#         cols_data = ["param", "unit", "val", "col"]
#         pd.DataFrame(columns=cols_data).to_sql(T.data, db.conn, index_label="hash2")

#     sql = f'SELECT DISTINCT ind.hash2, ind.hash, fname, ext, link \
#         FROM {T.wvg_inf} wi \
#         LEFT JOIN {T.wvg_lau} wl \
#             ON wi.WVG=wl.WVG \
#         LEFT JOIN {T.scrape} sc\
#             ON wl.LAU=sc.LAU\
#         LEFT JOIN {T.ind} ind \
#             ON sc.hash=ind.hash\
#         LEFT JOIN {T.extr} con \
#             ON ind.hash2 = con.hash2 \
#         LEFT JOIN {T.clean} cl \
#             ON con.hash2 = cl.hash2 \
#         LEFT JOIN {T.info} inf \
#             ON inf.hash2=con.hash2\
#         WHERE cl.hash2 IS NULL \
#             AND converter <> "ERR" \
#             AND con.params <> "0/0"\
#             AND inf.file <> "SCAN" \
#         ORDER BY wi.discharge DESC'

#     to_clean = pd.read_sql(sql, db.conn)

#     driver = webdriver.Chrome(service=SERVICE)
#     # return to_clean
#     for i, tups in enumerate(to_clean.itertuples()):
#         ind, hash2, path, fname, ext, link = tups

#         stat = {key: "" for key in cols}

#         print(f"{i}/{to_clean.index.size}: {path}/{fname}{ext}")
#         excel_path = os.path.join("cleaned", f"{hash2}.xlsx")

#         if not os.path.exists(excel_path):
#             try:
#                 path_extr = os.path.join("converted", f"{hash2}.xlsx")
#                 data = pd.read_excel(path_extr, header=0, index_col=0, sheet_name=None)
#                 stat["tabs_total"] = len(data)
#                 tables = find_tables(data, **kwargs)
#                 stat["tabs_dropped"] = len(data) - len(tables)
#                 tables, stats = clean_tables(tables)
#                 stat.update(stats)
#                 if tables == []:
#                     continue

#                 table = pd.concat(tables, axis=0)
#                 table = expand_duplicate_index(table)
#                 clean_table(table)

#                 if table.empty:
#                     continue

#                 stat["cleaned"] = "YES"

#             except ValueError:
#                 print("ERR")
#                 stat["cleaned"] = "ERR"
#                 continue

#             finally:
#                 stat_df = pd.DataFrame.from_dict(
#                     stat, columns=[hash2], orient="index"
#                 ).T
#                 stat_df.to_sql(
#                     T.clean, db.conn, if_exists="append", index_label=["hash2"]
#                 )

#             table.to_excel(excel_path)

#         if not db.read(f'SELECT hash2 from {T.data} WHERE hash2="{hash2}"').empty:
#             continue
#         sql = f'SELECT ex.hash2, date, ex.params, distr FROM extracted_tables ex\
#             LEFT JOIN file_info inf \
#                 ON  ex.hash2=inf.hash2 \
#             WHERE ex.hash2="{hash2}"'
#         conversion_data = db.read(sql)
#         print(conversion_data.T)

#         driver.get(link)
#         orig_path = os.path.join(path, f"{fname}{ext}")
#         os.startfile(orig_path)
#         os.startfile(excel_path)

#         date = input("Data status")
#         if date in ["q"]:
#             os.chdir("..")
#             driver.close()
#             return

#         elif date in ["skip"]:
#             continue

#         elif date in ["q", "quit", "break"]:
#             break

#         elif date in [""]:
#             date = conversion_data.date.values

#         db.exe(
#             f'UPDATE file_info SET date="{date}", date_orig="manual" WHERE hash2="{hash2}"'
#         )
#         df = pd.read_excel(excel_path, index_col=[0, 1], header=0)
#         for i, col in enumerate(df):
#             temp = pd.DataFrame(df[col])  # .rename({col: 'val'}, axis=1)
#             temp.rename({col: "val"}, axis=1, inplace=True)
#             temp["col"] = i
#             temp["hash2"] = hash2
#             temp.dropna().to_sql(T.data, db.conn, if_exists="append")

#         stat_df = pd.DataFrame.from_dict(stat, columns=[hash2], orient="index").T

#         stat_df.to_sql(T.clean, db.conn, if_exists="append", index_label=["hash2"])
#         clear_output(wait=False)
#     driver.close()
#     os.chdir("..")


# def read_params(query, overwrite=False, **kwargs):
#     os.chdir(query)

#     db = open_db()

#     if overwrite and T.clean in db.tables:
#         db.exe(f"DROP TABLE {T.clean}")

#     cols = ["cleaned", "tabs_total", "tabs_dropped", "method", "legal_lim"]
#     if T.clean not in db.tables:
#         pd.DataFrame(columns=cols).to_sql(T.clean, db.conn, index_label="hash2")

#     if T.extr not in db.tables:
#         print("No converted tables!")
#         return

#     if T.mapping not in db.tables:
#         cols_data = ["comm", "distr", "col"]
#         pd.DataFrame(columns=cols_data).to_sql(T.mapping, db.conn, index_label="hash2")

#     if T.data not in db.tables:
#         cols_data = ["param", "unit", "val", "col"]
#         pd.DataFrame(columns=cols_data).to_sql(T.data, db.conn, index_label="hash2")

#     sql = f'SELECT DISTINCT ind.hash2, ind.hash, fname, ext, link \
#         FROM {T.wvg_inf} wi \
#         LEFT JOIN {T.wvg_lau} wl \
#             ON wi.WVG=wl.WVG \
#         LEFT JOIN {T.scrape} sc\
#             ON wl.LAU=sc.LAU\
#         LEFT JOIN {T.ind} ind \
#             ON sc.hash=ind.hash\
#         LEFT JOIN {T.extr} con \
#             ON ind.hash2 = con.hash2 \
#         LEFT JOIN {T.clean} cl \
#             ON con.hash2 = cl.hash2 \
#         LEFT JOIN {T.info} inf \
#             ON inf.hash2=con.hash2\
#         WHERE cl.hash2 IS NULL \
#             AND converter <> "ERR" \
#             AND con.params <> "0/0"\
#             AND inf.file <> "SCAN" \
#         ORDER BY wi.discharge DESC'

#     to_clean = pd.read_sql(sql, db.conn)

#     for i, tups in enumerate(to_clean.itertuples()):
#         ind, hash2, path, fname, ext, link = tups

#         stat = {key: "" for key in cols}

#         print(f"{i}/{to_clean.index.size}: {path}/{fname}{ext}")
#         excel_path = os.path.join("cleaned", f"{hash2}.xlsx")

#         if not os.path.exists(excel_path):
#             try:
#                 path_extr = os.path.join("converted", f"{hash2}.xlsx")
#                 data = pd.read_excel(path_extr, header=0, index_col=0, sheet_name=None)
#                 tables = find_tables(data, **kwargs)
#                 tables, stats = clean_tables(tables)
#                 if tables == []:
#                     continue

#                 table = pd.concat(tables, axis=0)
#                 table = expand_duplicate_index(table)
#                 clean_table(table)

#                 if table.empty:
#                     continue

#                 stat["cleaned"] = "PARAM"

#             except ValueError:
#                 print("ERR")
#                 stat["cleaned"] = "ERR"
#                 continue

#             finally:
#                 stat_df = pd.DataFrame.from_dict(
#                     stat, columns=[hash2], orient="index"
#                 ).T
#                 stat_df.to_sql(
#                     T.clean, db.conn, if_exists="append", index_label=["hash2"]
#                 )
#             #
#             table.to_excel(excel_path)
#         else:
#             print("Already cleaned")
#             continue

#         if not db.read(f'SELECT hash2 from {T.data} WHERE hash2="{hash2}"').empty:
#             continue

#         df = pd.read_excel(excel_path, index_col=[0, 1], header=0)

#         temp = df.reset_index()[["param", "unit"]]
#         temp.unit = temp.unit.replace(regex=TO_REPLACE["unit"])
#         temp.param = temp.param.replace(regex=TO_REPLACE["index"])
#         temp["val"] = ""
#         temp["col"] = ""
#         temp["hash2"] = hash2
#         temp.to_sql(T.data, db.conn, index=False, if_exists="append")
#         clear_output(wait=False)
#     os.chdir("..")


# def assign_tables_sql(query):
#     os.chdir(query)
#     db = open_db()
#     driver = webdriver.Chrome(service=SERVICE)
#     sql = (
#         sql
#     ) = f'SELECT DISTINCT ind.hash2, ind.hash, fname, ext, link \
#         FROM {T.wvg_inf} wi \
#         LEFT JOIN {T.wvg_lau} wl \
#             ON wi.WVG=wl.WVG \
#         LEFT JOIN {T.scrape} sc\
#             ON wl.LAU=sc.LAU\
#         LEFT JOIN {T.ind} ind \
#             ON sc.hash=ind.hash\
#         LEFT JOIN {T.extr} con \
#             ON ind.hash2 = con.hash2 \
#         LEFT JOIN {T.mapping} m \
#             ON con.hash2 = m.hash2 \
#         LEFT JOIN {T.info} inf \
#             ON inf.hash2=con.hash2 \
#         LEFT JOIN {T.clean} cl \
#             ON cl.hash2=inf.hash2\
#         WHERE m.hash2 IS NULL \
#             AND cl.hash2 IS NOT NULL \
#             AND converter <> "ERR" \
#             AND con.params <> "0/0" \
#             AND inf.file <> "SCAN" \
#         ORDER BY wi.discharge DESC'

#     to_assign = db.read(sql)
#     for i, tups in enumerate(to_assign.itertuples()):
#         ind, hash2, path, fname, ext, link = tups

#         excel_path = os.path.join("cleaned", f"{hash2}.xlsx")
#         df = pd.read_excel(excel_path, index_col=[0, 1], header=0)

#         driver.get(link)
#         orig_path = os.path.join(path, f"{fname}{ext}")
#         os.startfile(orig_path)
#         os.startfile(excel_path)

#         for i, col in enumerate(df):
#             temp = pd.DataFrame(df[col])  # .rename({col: 'val'}, axis=1)
#             temp.rename({col: "val"}, axis=1, inplace=True)
#             temp["col"] = i
#             temp["hash2"] = hash2
#             temp.dropna().to_sql(T.data, db.conn, if_exists="append")
#             if col == "BG":
#                 mapping = {"comm": "BG", "distr": "", "col": i}
#                 mapping = pd.DataFrame.from_dict(
#                     mapping, orient="index", columns=[hash2]
#                 ).T
#                 mapping.to_sql(
#                     "mapping", db.conn, if_exists="append", index_label=["hash2"]
#                 )
#             locs = [loc for loc in re.split(PATS["comm"], col) if loc]
#             for loc in locs:
#                 pat = "(?P<comm>.*)\s*(?::|OT)\s*(?P<distr>.*)\s*"
#                 match = re.search(pat, loc, flags=re.DOTALL)
#                 if match:
#                     comm, dists = match.group("comm"), match.group("distr")
#                     dists = [dist for dist in re.split(PATS["distr"], dists) if dist]
#                 else:
#                     comm = loc
#                     dists = [""]
#                 for dist in dists:
#                     mapping = {"comm": comm, "distr": dist, "col": i}
#                     mapping = pd.DataFrame.from_dict(
#                         mapping, orient="index", columns=[hash2]
#                     ).T
#                     mapping.to_sql(
#                         "mapping", db.conn, if_exists="append", index_label=["hash2"]
#                     )


# def drop_limit_col(df: pd.DataFrame, session: Session) -> bool:
#     temp = df.reset_index()
#     mask = pd.DataFrame().reindex_like(temp)
#     for column in temp.columns:
#         for i, param, unit in temp.itertuples():
#             param = get_param(param, session=session)
#             if not param:
#                 continue
#             cell = pd.to_numeric(temp.loc[i, column], errors="ignore")
#             mask.loc[i, column] = (cell == param.limit) & (unit == param.unit)
#     sums = mask.sum().astype(int)
#     if sums.max() > 0:
#         idxmax = sums.idxmax()
#         df.drop(idxmax, axis=1, inplace=True)
#         return True
#     else:
#         return False


# def clean_table(table: pd.DataFrame, thresh: float = 0.75) -> None:
#     table.dropna(axis=1, how="all", inplace=True)
#     table.dropna(axis=0, how="all", inplace=True)

# def get_param(param: str, session: Session):
#     stmt = (
#         select(Param.param, Param.regex)
#         .where(Param.limit != None, Param.param != None)
#         .distinct()
#     )
#     params = [set()]
#     for par, regex in session.execute(stmt).scalars():
#         if not regex:
#             regex = re.escape(par)
#         if re.search(regex, param, flags=re.I):
#             params.append(par)
#     if len(params) == 1:
#         return params[0]


# def expand_duplicate_index(df: pd.DataFrame) -> pd.DataFrame:
#     no_dups = []
#     residue = df.copy()
#     while True:
#         dups = residue.index.duplicated()
#         no_dup = residue[~dups]
#         no_dups.append(no_dup)
#         residue = residue[dups]
#         if residue.empty:
#             break
#     for i, df in enumerate(no_dups):
#         oldies = df.columns.to_list()
#         newbs = [f"{old}.{i}" for old in oldies]
#         df.rename({old: new for old, new in zip(oldies, newbs)}, axis=1, inplace=True)

#     new = no_dups[0].copy()
#     for i, df in enumerate(no_dups):
#         if i == 0:
#             continue
#         else:
#             new = new.merge(df, left_index=True, right_index=True, how="outer")
#     return new


# def rescale_value(value: str or float or int, factor: float = 1):
#     if is_number(value):
#         if type(value) == str:
#             values = re.sub(r"\n.*|\s", "", value)
#             values = re.split("--|~", values)
#             floats = [float(clean_string(val)) * factor for val in values if val != ""]
#             rescaled = [
#                 f"{num:.{prec(val,factor)}f}" for num, val in zip(floats, values)
#             ]
#             return " ~ ".join(rescaled)
#         if type(value) == float or type(value) == int:
#             return value * factor
#         else:
#             return value
#     else:
#         return value


# def prec(num: str, factor: float) -> int:

#     split = num.split(".")
#     if len(split) == 2:
#         l_num = -1 * len(split[1])

#     else:
#         l_num = len(split[0]) - 1
#         if split[0].startswith("-"):
#             l_num -= 1

#     zeros = int(l_num + np.log10(np.abs(factor)))
#     if zeros > 0:
#         return 0
#     else:
#         return -zeros


# def clean_string(string: str) -> str:
#     string = string.replace(",", ".")
#     if "-" in string:
#         string = "-" + string.strip("-")
#     if string.count(".") > 1:
#         dot = string.find(".")
#         string = string.replace(".", "")
#         string = string[:dot] + "." + string[dot:]
#     return string
