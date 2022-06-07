# -*- coding: utf-8 -*-
"""
Created on Thu Feb 10 10:55:46 2022

@author: Leon
"""

from .complements import THRESH, KEYWORDS, TO_REPLACE, UNITS, MOLAR_MASS, SERVICE, PATS
from .database import open_db, T
from .index_utils import get_col, split_lengthwise, get_orientation, make_index_col
from IPython.display import clear_output
import numpy as np
import os
import pandas as pd
from pint import UnitRegistry
import re
from selenium import webdriver

ureg = UnitRegistry()


def clean_tables_sql(query, overwrite=False, **kwargs):
    os.chdir(query)

    db = open_db()

    if overwrite and T.clean in db.tables:
        db.exe(f'DROP TABLE {T.clean}')

    cols = ['cleaned', 'tabs_total', 'tabs_dropped', 'method', 'legal_lim']
    if T.clean not in db.tables:
        pd.DataFrame(columns=cols).to_sql(
            T.clean, db.conn, index_label='hash2')

    if T.extr not in db.tables:
        print('No converted tables!')
        return

    if T.mapping not in db.tables:
        cols_data = ['comm', 'distr', 'col']
        pd.DataFrame(columns=cols_data).to_sql(
            T.mapping, db.conn, index_label='hash2')

    if T.data not in db.tables:
        cols_data = ['param', 'unit', 'val', 'col']
        pd.DataFrame(columns=cols_data).to_sql(
            T.data, db.conn, index_label='hash2')

    sql = f'SELECT DISTINCT ind.hash2, ind.hash, fname, ext, link \
        FROM {T.wvg_inf} wi \
        LEFT JOIN {T.wvg_lau} wl \
            ON wi.WVG=wl.WVG \
        LEFT JOIN {T.scrape} sc\
            ON wl.LAU=sc.LAU\
        LEFT JOIN {T.ind} ind \
            ON sc.hash=ind.hash\
        LEFT JOIN {T.extr} con \
            ON ind.hash2 = con.hash2 \
        LEFT JOIN {T.clean} cl \
            ON con.hash2 = cl.hash2 \
        LEFT JOIN {T.info} inf \
            ON inf.hash2=con.hash2\
        WHERE cl.hash2 IS NULL \
            AND converter <> "ERR" \
            AND con.params <> "0/0"\
            AND inf.file <> "SCAN" \
        ORDER BY wi.discharge DESC'

    to_clean = pd.read_sql(sql, db.conn)

    driver = webdriver.Chrome(service=SERVICE)
    # return to_clean
    for i, tups in enumerate(to_clean.itertuples()):
        ind, hash2, path, fname, ext, link = tups

        stat = {key: '' for key in cols}

        print(f'{i}/{to_clean.index.size}: {path}/{fname}{ext}')
        excel_path = os.path.join('cleaned', f'{hash2}.xlsx')

        if not os.path.exists(excel_path):
            try:
                path_extr = os.path.join('converted', f'{hash2}.xlsx')
                data = pd.read_excel(path_extr, header=0,
                                     index_col=0, sheet_name=None)
                stat['tabs_total'] = len(data)
                tables = find_tables(data, **kwargs)
                stat['tabs_dropped'] = len(data) - len(tables)
                tables, stats = clean_tables(tables)
                stat.update(stats)
                if tables == []:
                    continue

                table = pd.concat(tables, axis=0)
                table = expand_duplicate_index(table)
                clean_table(table)

                if table.empty:
                    continue

                stat['cleaned'] = 'YES'

            except ValueError:
                print('ERR')
                stat['cleaned'] = 'ERR'
                continue

            finally:
                stat_df = pd.DataFrame.from_dict(
                    stat, columns=[hash2], orient='index').T
                stat_df.to_sql(T.clean, db.conn, if_exists='append',
                               index_label=['hash2'])

            table.to_excel(excel_path)

        if not db.read(f'SELECT hash2 from {T.data} WHERE hash2="{hash2}"').empty:
            continue
        sql = f'SELECT ex.hash2, date, ex.params, distr FROM extracted_tables ex\
            LEFT JOIN file_info inf \
                ON  ex.hash2=inf.hash2 \
            WHERE ex.hash2="{hash2}"'
        conversion_data = db.read(sql)
        print(conversion_data.T)

        driver.get(link)
        orig_path = os.path.join(path, f'{fname}{ext}')
        os.startfile(orig_path)
        os.startfile(excel_path)

        date = input('Data status')
        if date in ['q']:
            os.chdir('..')
            driver.close()
            return

        elif date in ['skip']:
            continue

        elif date in ['q', 'quit', 'break']:
            break

        elif date in ['']:
            date = conversion_data.date.values

        db.exe(
            f'UPDATE file_info SET date="{date}", date_orig="manual" WHERE hash2="{hash2}"')
        df = pd.read_excel(
            excel_path, index_col=[0, 1], header=0)
        for i, col in enumerate(df):
            temp = pd.DataFrame(df[col])  # .rename({col: 'val'}, axis=1)
            temp.rename({col: 'val'}, axis=1, inplace=True)
            temp['col'] = i
            temp['hash2'] = hash2
            temp.dropna().to_sql(T.data, db.conn, if_exists='append')

        stat_df = pd.DataFrame.from_dict(
            stat, columns=[hash2], orient='index').T

        stat_df.to_sql(T.clean, db.conn, if_exists='append',
                       index_label=['hash2'])
        clear_output(wait=False)
    driver.close()
    os.chdir('..')


def read_params(query, overwrite=False, **kwargs):
    os.chdir(query)

    db = open_db()

    if overwrite and T.clean in db.tables:
        db.exe(f'DROP TABLE {T.clean}')

    cols = ['cleaned', 'tabs_total', 'tabs_dropped', 'method', 'legal_lim']
    if T.clean not in db.tables:
        pd.DataFrame(columns=cols).to_sql(
            T.clean, db.conn, index_label='hash2')

    if T.extr not in db.tables:
        print('No converted tables!')
        return

    if T.mapping not in db.tables:
        cols_data = ['comm', 'distr', 'col']
        pd.DataFrame(columns=cols_data).to_sql(
            T.mapping, db.conn, index_label='hash2')

    if T.data not in db.tables:
        cols_data = ['param', 'unit', 'val', 'col']
        pd.DataFrame(columns=cols_data).to_sql(
            T.data, db.conn, index_label='hash2')

    sql = f'SELECT DISTINCT ind.hash2, ind.hash, fname, ext, link \
        FROM {T.wvg_inf} wi \
        LEFT JOIN {T.wvg_lau} wl \
            ON wi.WVG=wl.WVG \
        LEFT JOIN {T.scrape} sc\
            ON wl.LAU=sc.LAU\
        LEFT JOIN {T.ind} ind \
            ON sc.hash=ind.hash\
        LEFT JOIN {T.extr} con \
            ON ind.hash2 = con.hash2 \
        LEFT JOIN {T.clean} cl \
            ON con.hash2 = cl.hash2 \
        LEFT JOIN {T.info} inf \
            ON inf.hash2=con.hash2\
        WHERE cl.hash2 IS NULL \
            AND converter <> "ERR" \
            AND con.params <> "0/0"\
            AND inf.file <> "SCAN" \
        ORDER BY wi.discharge DESC'

    to_clean = pd.read_sql(sql, db.conn)

    for i, tups in enumerate(to_clean.itertuples()):
        ind, hash2, path, fname, ext, link = tups

        stat = {key: '' for key in cols}

        print(f'{i}/{to_clean.index.size}: {path}/{fname}{ext}')
        excel_path = os.path.join('cleaned', f'{hash2}.xlsx')

        if not os.path.exists(excel_path):
            try:
                path_extr = os.path.join('converted', f'{hash2}.xlsx')
                data = pd.read_excel(path_extr, header=0,
                                     index_col=0, sheet_name=None)
                tables = find_tables(data, **kwargs)
                tables, stats = clean_tables(tables)
                if tables == []:
                    continue

                table = pd.concat(tables, axis=0)
                table = expand_duplicate_index(table)
                clean_table(table)

                if table.empty:
                    continue

                stat['cleaned'] = 'PARAM'

            except ValueError:
                print('ERR')
                stat['cleaned'] = 'ERR'
                continue

            finally:
                stat_df = pd.DataFrame.from_dict(
                    stat, columns=[hash2], orient='index').T
                stat_df.to_sql(T.clean, db.conn, if_exists='append',
                               index_label=['hash2'])
#
            table.to_excel(excel_path)
        else:
            print('Already cleaned')
            continue

        if not db.read(f'SELECT hash2 from {T.data} WHERE hash2="{hash2}"').empty:
            continue

        df = pd.read_excel(
            excel_path, index_col=[0, 1], header=0)

        temp = df.reset_index()[['param', 'unit']]
        temp.unit = temp.unit.replace(regex=TO_REPLACE['unit'])
        temp.param = temp.param.replace(regex=TO_REPLACE['index'])
        temp['val'] = ''
        temp['col'] = ''
        temp['hash2'] = hash2
        temp.to_sql(T.data, db.conn, index=False, if_exists='append')
        clear_output(wait=False)
    os.chdir('..')


def assign_tables_sql(query):
    os.chdir(query)
    db = open_db()
    driver = webdriver.Chrome(service=SERVICE)
    sql = sql = f'SELECT DISTINCT ind.hash2, ind.hash, fname, ext, link \
        FROM {T.wvg_inf} wi \
        LEFT JOIN {T.wvg_lau} wl \
            ON wi.WVG=wl.WVG \
        LEFT JOIN {T.scrape} sc\
            ON wl.LAU=sc.LAU\
        LEFT JOIN {T.ind} ind \
            ON sc.hash=ind.hash\
        LEFT JOIN {T.extr} con \
            ON ind.hash2 = con.hash2 \
        LEFT JOIN {T.mapping} m \
            ON con.hash2 = m.hash2 \
        LEFT JOIN {T.info} inf \
            ON inf.hash2=con.hash2 \
        LEFT JOIN {T.clean} cl \
            ON cl.hash2=inf.hash2\
        WHERE m.hash2 IS NULL \
            AND cl.hash2 IS NOT NULL \
            AND converter <> "ERR" \
            AND con.params <> "0/0" \
            AND inf.file <> "SCAN" \
        ORDER BY wi.discharge DESC'

    to_assign = db.read(sql)
    for i, tups in enumerate(to_assign.itertuples()):
        ind, hash2, path, fname, ext, link = tups

        excel_path = os.path.join('cleaned', f'{hash2}.xlsx')
        df = pd.read_excel(
            excel_path, index_col=[0, 1], header=0)

        driver.get(link)
        orig_path = os.path.join(path, f'{fname}{ext}')
        os.startfile(orig_path)
        os.startfile(excel_path)

        for i, col in enumerate(df):
            temp = pd.DataFrame(df[col])  # .rename({col: 'val'}, axis=1)
            temp.rename({col: 'val'}, axis=1, inplace=True)
            temp['col'] = i
            temp['hash2'] = hash2
            temp.dropna().to_sql(T.data, db.conn, if_exists='append')
            if col == 'BG':
                mapping = {'comm': 'BG',
                           'distr': '',
                           'col': i}
                mapping = pd.DataFrame.from_dict(
                    mapping, orient='index', columns=[hash2]).T
                mapping.to_sql('mapping', db.conn,
                               if_exists='append', index_label=['hash2'])
            locs = [loc for loc in re.split(PATS['comm'], col) if loc]
            for loc in locs:
                pat = '(?P<comm>.*)\s*(?::|OT)\s*(?P<distr>.*)\s*'
                match = re.search(pat, loc, flags=re.DOTALL)
                if match:
                    comm, dists = match.group('comm'), match.group('distr')
                    dists = [dist for dist in re.split(
                        PATS['distr'], dists) if dist]
                else:
                    comm = loc
                    dists = ['']
                for dist in dists:
                    mapping = {'comm': comm,
                               'distr': dist,
                               'col': i}
                    mapping = pd.DataFrame.from_dict(
                        mapping, orient='index', columns=[hash2]).T
                    mapping.to_sql('mapping', db.conn,
                                   if_exists='append', index_label=['hash2'])


def clean_data(df):
    new = df.copy()
    new = new.replace('', np.nan)
    if new.empty:
        return
    index, axis = get_orientation(new, KEYWORDS['params'])
    if axis == 1:
        new = new.T
    data_cols = new.columns.difference(index)
    new[data_cols] = new[data_cols].replace(regex=[',', '-|–', r'<\s*', r'˂\s*'],
                                            value=['.', '--', '-', '-'])
    return new


# def clean_tables(tables, thresh=0.1, **kwargs):
#     stats = {}
#     out = []
#     for table in tables:
#         table = split_lengthwise(table)
#         convert_units(table, thresh=0.1)
#         for key in ['method', 'unit', 'number_col']:
#             stats[key] = drop_col(table, KEYWORDS[key], thresh=0.1)
#         stats['legal_lim'] = drop_limit_col(table)
#         drop_string_cols(table)

#         pat = r'(?<=\d)\D*\Z'
#         table.replace(to_replace=None, value=[''], regex=[pat], inplace=True)
#         table.dropna(axis=1, how='all')
#         table.dropna(axis=0, how='all')

#         old_index = table.columns.get_level_values(2)
#         new_index = pd.RangeIndex(stop=table.shape[1])
#         table.rename({old: new for old, new in zip(
#         out.append(table)
#     return out


def clean_tables(tables, thresh=0.1, **kwargs):
    stats = {}
    out = []
    stats['method'] = 0
    stats['legal_lim'] = 0
    for table in tables:
        table = split_lengthwise(table)
        make_unit_col(table)
        make_index_col(table, thresh=0.2)

        if 'param' in table and 'unit' in table:
            table.set_index(['param', 'unit'], inplace=True)
        elif 'param' in table:
            table.set_index(['param'], inplace=True)
        else:
            continue
        convert_units(table, thresh=0.1)
        stats['method'] += drop_col(table, KEYWORDS['method'], thresh=0.1)
        if 'unit' in table.index.names:
            stats['legal_lim'] += drop_limit_col(table)
        drop_string_cols(table)

        table.dropna(axis=1, how='all')
        table.dropna(axis=0, how='all')
        table.rename({old: new for old, new in zip(
            table.columns, np.arange(table.columns.size))}, axis=1, inplace=True)
        out.append(table)
    return out, stats


def clean_table(table, thresh=0.75):
    table.dropna(axis=1, how='all', inplace=True)
    table.dropna(axis=0, how='all', inplace=True)


def drop_col(data, keys, **kwargs):
    index = get_col(data, keys, **kwargs)
    if not index.empty:
        data.drop(index, axis=1, inplace=True)
        return True
    else:
        return False


def drop_limit_col(df):
    temp = df.reset_index()
    mask = pd.DataFrame().reindex_like(temp)
    for column in temp.columns:
        for vals in temp.itertuples():
            i, param, unit = vals.Index, vals.param, vals.unit
            if (param not in THRESH) or (param not in UNITS):
                continue
            cell = pd.to_numeric(
                temp.loc[i, column], errors='ignore')
            mask.loc[i, column] = (
                cell == THRESH[param]) & (unit == UNITS[param])
    sums = mask.sum().astype(int)
    if sums.max() > 0:
        idxmax = sums.idxmax()
        df.drop(idxmax, axis=1, inplace=True)
        return True
    else:
        return False


def drop_string_cols(df, thresh=0.75):
    mask = ~df.applymap(is_number)
    rel_mask = mask.sum()/df.count()
    index_col = get_col(df, KEYWORDS['params'], thresh=0.1)
    index = rel_mask[rel_mask > thresh].index.difference(index_col)
    df.drop(index, axis=1, inplace=True)


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

def expand_duplicate_index(df):
    no_dups = []
    residue = df.copy()
    while True:
        dups = residue.index.duplicated()
        no_dup = residue[~dups]
        no_dups.append(no_dup)
        residue = residue[dups]
        if residue.empty:
            break
    for i, df in enumerate(no_dups):
        oldies = df.columns.to_list()
        newbs = [f'{old}.{i}' for old in oldies]
        df.rename({old: new for old, new in zip(
            oldies, newbs)}, axis=1, inplace=True)

    new = no_dups[0].copy()
    for i, df in enumerate(no_dups):
        if i == 0:
            continue
        else:
            new = new.merge(df, left_index=True, right_index=True, how='outer')
    return new


def rescale_value(value, factor=1):
    if is_number(value):
        if type(value) == str:
            values = re.sub(r'\n.*|\s', '', value)
            values = re.split('--|~', values)
            floats = [float(clean_string(val)) *
                      factor for val in values if val != '']
            rescaled = [f'{num:.{prec(val,factor)}f}' for num,
                        val in zip(floats, values)]
            return ' ~ '.join(rescaled)
        if type(value) == float or type(value) == int:
            return value * factor
        else:
            return value
    else:
        return value


def prec(num, factor):

    split = num.split('.')
    if len(split) == 2:
        l_num = -1*len(split[1])

    else:
        l_num = len(split[0])-1
        if split[0].startswith('-'):
            l_num -= 1

    zeros = int(l_num + np.log10(np.abs(factor)))
    if zeros > 0:
        return 0
    else:
        return -zeros


def clean_string(string):
    string = string.replace(',', '.')
    if '-' in string:
        string = '-'+string.strip('-')
    if string.count('.') > 1:
        dot = string.find('.')
        string = string.replace('.', '')
        string = string[:dot]+'.'+string[dot:]
    return string


def is_number(value):
    value = str(value)
    pat = r'\A([~\s+-]*(\d)+(\.\d+)?[~\s+-±]*)*\Z'
    if re.match(pat, value) or value == 'nan':
        return True
    else:
        return False


def clean_index(data, **kwargs):
    col = get_col(data, KEYWORDS['params'], **kwargs)
    data[col] = data[col].fillna(
        data.index.to_series()).replace(regex=TO_REPLACE['index'])


# def convert_units(df, **kwargs):
#     unit_col = get_col(df, KEYWORDS['unit'], **kwargs)
#     index_col = get_col(df, KEYWORDS['params'], **kwargs)
#     if unit_col.empty or index_col.empty or unit_col.size > 1:
#         return df
#     clean_index(df, **kwargs)
#     for index, values in df.dropna(subset=unit_col).iterrows():
#         unit, = values.loc[unit_col].to_list()
#         param, = values.loc[index_col].to_list()

#         if param == []:
#             continue
#         if param in UNITS:
#             default_unit = UNITS[param]
#             factor = compare_units(unit, default_unit, param)
#             if factor:
#                 df.loc[index] = values.apply(rescale_value, factor=factor)
#                 df.loc[index, unit_col] = default_unit


def convert_units(df, **kwargs):
    if 'param' not in df.index.names or 'unit' not in df.index.names:
        return
    df.reset_index(inplace=True)
    df.set_index('param', inplace=True)
    for i, (index, values) in enumerate(df.iterrows()):
        param = index
        unit = values['unit']
        if param in UNITS and type(unit) == str:
            default_unit = UNITS[param]
            factor = compare_units(unit, default_unit, param)
            if factor:
                df.iloc[i] = values.apply(rescale_value, factor=factor)
                df.iloc[i, 0] = default_unit
    df.reset_index(inplace=True)
    df.set_index(['param', 'unit'], inplace=True)


def compare_units(unit, default_unit, param):
    factor = 1
    if not unit and not default_unit:
        return None

    if 'mol' in unit and param in MOLAR_MASS:
        unit = re.sub('mol', 'g', unit)
        factor *= MOLAR_MASS[param]

    try:
        if (unit in ureg) and (default_unit in ureg):
            factor *= ureg.Quantity(unit).to(default_unit).magnitude
            return factor
    except Exception:
        return None


def make_unit_col(df, **kwargs):
    df.replace(value=0, regex=KEYWORDS['nan'], inplace=True)
    index_col = get_col(df, KEYWORDS['params'], thresh=0.2)
    unit_col = get_col(df, KEYWORDS['unit'], **kwargs)
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
            unit_df = df.drop(index_col, axis=1).replace(regex=TO_REPLACE['unit']).apply(lambda x: x.dropna().astype(
                str).str.findall(r'\D+\Z').str.join(''))
            units = unit_df.T.apply(
                lambda x: x.value_counts().idxmax())
        else:
            unit_col = get_col(df, KEYWORDS['unit'], thresh=1)
            if unit_col.size == 1:
                row = get_col(df.T, KEYWORDS['unit'], thresh=1)
                if row.size == 1:
                    unit = df.loc[row, unit_col].values
                    units = pd.Series(np.full(df.index.size, unit))
                else:
                    units = pd.Series(np.full(df.index.size, '?'))
            else:
                units = pd.Series(np.full(df.index.size, '?'))
    df.insert(0, 'unit', units.replace(
        regex=TO_REPLACE['unit']).fillna(method='ffill'))
    pat = r'(?<=\d)\s*[A-Za-z].*\Z'
    df.replace(value=[''], regex=[pat], inplace=True)
    return df


def find_tables(tables):
    out = []
    keys = '|'.join(KEYWORDS['params'])
    for key, table in tables.items():
        string = table.apply(lambda x: x.astype(str).str.contains(
            f'{keys}'))
        if np.any(string):
            out.append(table)
    return out
