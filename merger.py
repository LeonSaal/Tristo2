# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 10:09:54 2022

@author: Leon
"""
from .cleaner import clean_tables, clean_table, expand_duplicate_index
from .complements import FOLDERS, KEYWORDS
from .converter import read_index_data
from .demog_data import get_districts
from .index_utils import path_from_index, set_index_col, get_col
from IPython.display import clear_output
import numpy as np
import os
import pandas as pd
import re
from sqlalchemy import create_engine, insert, inspect


def merge_sql(query):
    os.chdir(query)

    engine = create_engine(f'sqlite:///{query}.db')
    conn = engine.connect()
    tables = inspect(engine).get_table_names()

    t_info = 'info_pdf'
    t_cl = 'cleaned_pdf'

    if t_cl not in tables:
        return

    years = pd.read_sql(f'SELECT DISTINCT date FROM {t_info} WHERE date', conn).sort_values(
        ascending=False)
    for year in years:
        with pd.Excelwriter('data.xlsx') as writer:
            fi

            data.to_excel(writer, sheet_name=year)


def merge_xlsxs(query, **kwargs):
    os.chdir(query)
    logger = make_logger('merger')
    stats = {}
    data_years = {}
    index_data = read_index_data()['.xlsx']
    years = index_data['date'].dropna().sort_values(
        ascending=False).astype(int).unique()
    for year in years:
        clear_output(wait=True)
        print(year)
        if year <= 2019:
            continue
        data_years[year] = []
        slices = index_data[index_data['date'] == year].copy()
        for ars in slices.index.get_level_values(0):
            dfs = []
            # return to_merge
            # return slices, ars
            for index in slices.loc[ars].index:
                print(index)
                # print((ars,)+index)
                path = path_from_index((ars,)+index)
                bpath, ext = os.path.splitext(path)
                xlsx = bpath+'.xlsx'
                # try:
                data = pd.read_excel(
                    xlsx, header=[0, 1, 2], index_col=0, sheet_name=None)
                tables = find_tables(data, **kwargs)
                tables = clean_tables(tables)

                if tables == []:
                    continue

                table = pd.concat(tables, axis=0, ignore_index=True)
                #table = expand_duplicate_index(table)
                table = set_index_col(table, thresh=0.1)
                clean_table(table)
                if table.empty:
                    continue

                logger.info(
                    ';'.join([str(x) for x in [ars]+list(index)+list(stats.values())]))
                # except:
                #     print(f'Unable to read {bpath}')
                #     continue
                if dfs == []:
                    dfs.append(table)
                else:
                    unique = True
                    for df in dfs:
                        if table.equals(df):
                            unique = False
                            break
                    if unique:
                        dfs.append(table)
            if dfs != []:
                path_temp = os.path.join(
                    FOLDERS['TEMP'], f'{year:.0f}_{ars}_{index[0]}.xlsx')
                data = pd.concat(dfs, axis=1)
                data.to_excel(path_temp)
                data_years[year].append(data)
        if len(data_years[year]) == 0:
            data_years.pop(year)
        else:
            data_years[year] = pd.concat(data_years[year], axis=1)
    with pd.ExcelWriter('data.xlsx') as writer:
        for year, df in data_years.items():
            df.drop_duplicates().to_excel(writer, sheet_name=str(year))
    os.chdir('..')


# def merge_xlsxs(**kwargs):
#     logger = make_logger('merger', 'MERGER')
#     stats = {}
#     to_merge = INDEX_DATA[(INDEX_DATA['analysis'] == 'YES') & (INDEX_DATA['converter'] != 'ERR') & (
#         INDEX_DATA['conversion_rate'] != '0/0') & (INDEX_DATA['file'] != 'ERR')].copy()
#     years = INDEX_DATA['date'].unique()
#     for year in years:
#         slices = to_merge[to_merge['date'] == year].copy()
#         for ars in slices.index.get_level_values(0)[:1]:
#             dfs = []
#             # return to_merge
#             # return slices, ars
#             for index in slices.loc[ars].index:
#                 print(index)
#                 # print((ars,)+index)
#                 path = path_from_index((ars,)+index)
#                 bpath, ext = os.path.splitext(path)
#                 xlsx = bpath+'.xlsx'
#                 # try:
#                 data = pd.read_excel(
#                     xlsx, header=[0, 1, 2], index_col=0, sheet_name=None)
#                 tables = find_tables(data, **kwargs)
#                 tables = clean_tables(tables)

#                 if tables == []:
#                     continue

#                 table = pd.concat(tables, axis=0, ignore_index=True)

#                 table = set_index_col(table, thresh=0.1)
#                 clean_table(table)
#                 if table.empty:
#                     continue

#                 logger.info(
#                     ';'.join([str(x) for x in [ars]+list(index)+list(stats.values())]))
#                 # except:
#                 #     print(f'Unable to read {bpath}')
#                 #     continue
#                 if dfs == []:
#                     dfs.append(table)
#                 else:
#                     unique = True
#                     for df in dfs:
#                         if table.equals(df):
#                             unique = False
#                             break
#                     if unique:
#                         dfs.append(table)
#             if dfs != []:
#                 path_temp = os.path.join(
#                     FOLDERS['TEMP'], f'{year:.0f}_{ars}_{index[0]}.xlsx')
#                 pd.concat(dfs, axis=1).to_excel(path_temp)

def merge_temp():
    os.chdir(FOLDERS['TEMP'])
    files = os.listdir()
    files = [pd.read_excel(file) for file in files if '.xlsx' in file]


def get_year(pdf):
    years = {}
    find = re.search(r'.*(?P<year>\d{4}).*')
    if find:
        years['fname'] = int(find.group('year'))


def find_districts(df):
    ars = df.columns.get_level_values(0).unique()[0]
    all_districts = get_districts(ars)
    if len(all_districts) == 0:
        return df
    pat = '|'.join(all_districts)
    new = pd.DataFrame()
    for i, (col, values) in enumerate(df.iteritems()):
        mask = values.astype(str).str.contains(pat)
        if mask.sum() > 0:
            districts = values.loc[mask].to_list()
            for district in districts:
                new.insert(i, district, values)
        else:
            new.insert(i, i, values)
    return new
