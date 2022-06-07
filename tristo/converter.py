# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 10:08:54 2022

@author: Leon
"""
from .database import get_from_hash, open_db, T
import fitz
from tabula import read_pdf
import re
from pathlib import Path
import pandas as pd
import os
from ocrmypdf import ocr
import ocrmypdf
import numpy as np
from IPython.display import clear_output
from .index_utils import path_from_index, get_index_from_path, make_file_index
from .complements import PATS, hashf
import camelot as cm
from.cleaner import clean_data

''
COLS_INFO = ['file', 'MB', 'pages',  'date', 'date_orig', 'params', 'distr', 'LAUs',
             'data_basis', 'legal_lim', 'analysis', 'OMP']

# def get_index(df, keys, escape=False):
#     if escape:
#         pat = '|'.join([re.escape(str(param)) for param in keys])
#     else:
#         pat = '|'.join([str(param) for param in keys])
#     mask = df.apply(lambda x: x.astype('string').str.contains(pat))
#     mask_sum_0 = mask.sum(axis=0)
#     mask_sum_1 = mask.sum(axis=1)

#     if mask_sum_1.max() > mask_sum_0.max():
#         index = mask_sum_1.idxmax()
#         score = f'{mask_sum_1.max()} / {len(df)}'
#         axis = 1
#     else:
#         index = mask_sum_0.idxmax()
#         axis = 0
#         score = f'{mask_sum_0.max()} / {len(df)}'

#     return index, axis, score


def extract_pdf(file, hash2, save=True, log=True, overwrite=False):
    '''
    extracts tables from .pdf and saves them as .xlsx.
    camelot  >> tabula

    Parameters
    ----------
    file : str
        path of file.
    save : bool, optional
        save results. The default is True.
    log : bool, optional
        log to file. The default is True.
    overwrite : bool, optional
        overwrite converted files. The default is False.

    Returns
    -------
    dict
        dict conatining converter and conversion rate.

    '''
    fname, ext = os.path.splitext(file)
    stat = {}
    if os.path.exists(fname+'.xlsx') and overwrite is False:
        return

    with fitz.open(file) as doc:
        text = get_text_pdf(doc)

    matches = re.findall(PATS['params'], re.sub('\n', ' ', text))
    n_matches = len(matches)

    extracted_tables = {'tables': {}, 'n_params': {}, 'cells': {}}
    try:
        tables = cm.read_pdf(file, pages='all',
                             flavor='stream', edge_tol=500, row_tol=5)

        tables = [table.df for table in tables]

        n_params = count_occ(tables, PATS['params'])
        extracted_tables['tables']['camelot'] = tables
        extracted_tables['n_params']['camelot'] = n_params
        extracted_tables['cells']['camelot'] = sum(
            [table.size for table in tables])
        if n_params < n_matches:

            raise Exception

    except Exception:
        try:
            tables = read_pdf(file, pages='all')
            tables = [table.dropna(how='all').dropna(axis=1, how='all')
                      for table in tables if table.notna().to_numpy().any()]
            n_params = count_occ(tables, PATS['params'])
            extracted_tables['tables']['tabula'] = tables
            extracted_tables['n_params']['tabula'] = n_params
            extracted_tables['cells']['camelot'] = sum(
                [table.size for table in tables])

        except Exception:
            return {'converter': 'ERR'}

    converter = max(extracted_tables['cells'],
                    key=extracted_tables['cells'].get)
    tables = extracted_tables['tables'][converter]
    n_params = extracted_tables['n_params'][converter]
    news = []
    err = 0
    n_tab = len(tables)
    if n_tab == 0.:
        return {'converter': 'ERR'}

    for table in tables:
        try:
            new = clean_data(table)
            news += [new]
        except Exception:
            err += 1
            continue

    if save and news != []:
        dst = os.path.join('..', 'converted', f'{hash2}')
        save_tables(news, dst)
    stat = {
        'converter': converter,
        'conversion_rate': f'{n_tab-err}/{n_tab}',
        'params': f'{n_params}/{n_matches}'}
    return stat


def extract_excel(file, hash2):
    tables = pd.read_excel(file, sheet_name=None)
    tables = [table for table in tables.values()]
    n_params = count_occ(tables, PATS['params'])

    news = []
    err = 0
    n_tab = len(tables)
    for table in tables:
        try:
            new = clean_data(table)
            news += [new]
        except ValueError:
            err += 1
            continue
    if news != []:
        dst = os.path.join('..', 'converted', f'{hash2}')
        save_tables(news, dst)
    stats = {
        'converter': 'NONE',
        'conversion_rate': f'{n_tab-err}/{n_tab}',
        'params': str(n_params)}

    return stats


def count_occ(dfs, pat):
    return sum([df.apply(lambda x: x.astype(str).str.contains(pat)).to_numpy().sum() for df in dfs])


# def make_logger(name):
#     '''
#     makes logger.

#     Parameters
#     ----------
#     name : str
#         name for logger.
#     handler : str
#         name of FileHandler.

#     Returns
#     -------
#     logger : logger
#         logger object.

#     '''
#     logger = logging.getLogger(name)
#     logger.setLevel(logging.DEBUG)

#     if name == 'stream':
#         handler = logging.StreamHandler()
#     else:
#         handler = logging.FileHandler(filename=name+'.log')
#     handler.setFormatter(LOG_FORMATS[name.upper()])
#     logger.addHandler(handler)
#     return logger

def extract_tables(query, overwrite=False, min_params=10, **kwargs):
    os.chdir(query)
    db = open_db()

    if not os.path.exists('converted'):
        os.mkdir('converted')

    if overwrite and T.extr in db.tables:
        db.conn.execute(f'DROP TABLE {T.extr}')

    if T.extr not in db.tables:
        pd.DataFrame(columns=['converter', 'conversion_rate', 'params']).to_sql(
            T.extr, db.conn, index_label='hash2')

    sql = f'SELECT ind.hash2, ind.hash, ind.fname, ind.ext \
            FROM {T.ind} ind \
            INNER JOIN {T.info} inf \
                ON ind.hash2 = inf.hash2 \
            LEFT JOIN {T.extr} e \
                ON ind.hash2 = e.hash2 \
            WHERE e.hash2 IS NULL \
                AND inf.file NOT IN ("ERR", "OMITTED") \
                AND inf.params > {min_params} \
                AND ind.ext IN (".pdf", ".xlsx", ".xls")'

    to_convert = pd.read_sql(sql, db.conn)

    for i, (tups) in enumerate(to_convert.itertuples()):
        ind, hash2, path, fname, ext = tups
        stat = {'converter': 'ERR',
                'conversion_rate': ''}

        print(f'{i+1}/{to_convert.index.size}: {path}/{fname}{ext}')
        os.chdir(path)
        file = f'{fname}{ext}'
        try:
            if ext == '.pdf':
                stat.update(extract_pdf(file, hash2, overwrite=True))

            elif ext in ['.xls', '.xlsx']:
                stat.update(extract_excel(file, hash2))

        except Exception:
            pass
        clear_output(wait=True)
        stats_df = pd.DataFrame.from_dict(
            stat, orient='index', columns=[hash2]).T
        stats_df.to_sql(T.extr, db.conn, if_exists='append',
                        index_label='hash2')
        os.chdir('..')
    os.chdir('..')
    return


def to_list(df):
    '''
    takes DataFrame with lists in cells
    and returns DataFrame with lists expanded to new columns

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with lists in cells.

    Returns
    -------
    pd.DataFrame
        DataFrame with lists expanded to columns.

    '''
    merge = []
    for col in df.columns:
        temp = df.loc[:, col].dropna(axis=0)
        split = pd.DataFrame(temp.to_list(), index=temp.index)
        merge += [split]
    return pd.concat(merge, axis=1, ignore_index=True)


def save_tables(tables, fname):
    '''
    takes list of DataFrames and saves them to sheets in a single .xlsx file.

    Parameters
    ----------
    tables : list of pd.DataFrames
        list of pd.DataFrames to be saved as  single excel file.
    fname : str
        filename.

    Returns
    -------
    None.

    '''
    with pd.ExcelWriter(fname+'.xlsx') as writer:
        for i, table in enumerate(tables):
            table.to_excel(writer, sheet_name=str(i))


def update_file_index(query, overwrite=False):
    os.chdir(query)
    folder = os.listdir()
    sites = len([folder for folder in folder if re.match(
        r'[a-f0-9]{40}', folder)])
    db = open_db()

    if overwrite and T.ind in db.tables:
        db.conn.execute(f'DROP TABLE {T.ind}')

    if T.ind not in db.tables:
        pd.DataFrame(columns=['fname', 'ext', 'hash2']).to_sql(
            T.ind, db.conn, index_label='hash')
    new_files = 0
    new_folders = 0
    found = 0
    for root, dirs, files in os.walk(os.getcwd()):
        idx = get_index_from_path(root)
        if idx:
            found += 1
            sql = f'SELECT fname, ext \
                FROM "{T.ind}" i \
                    WHERE i.hash = "{idx}"'
            ex_files = pd.read_sql(sql, db.conn).sum(axis=1).values
            if db.read(f'SELECT hash FROM file_index WHERE hash="{idx}"').empty:
                new_folders += 1
        else:
            continue
        print(f'{found}/{sites}: {idx}')
        matches = [os.path.splitext(file)
                   for file in files if file not in ex_files]
        if matches == []:
            clear_output(wait=True)
            continue
        fnames = pd.DataFrame(matches, columns=['fname', 'ext'])
        to_update = make_file_index(fnames.fname, idx).reset_index()
        to_update['ext'] = fnames.ext
        to_update['hash2'] = (to_update.astype(
            str).sum(axis=1)).apply(hashf).values
        to_update.to_sql(T.ind, db.conn, if_exists='append', index=False)
        new_files += len(fnames)
        clear_output(wait=True)
    db.conn.close()
    print(f'{new_folders} new folders and {new_files} new files')
    os.chdir('..')


def removesuffix(string, suffix):
    if string.endswith(suffix):
        return string[: -len(suffix)]
    else:
        return string


def extract_pdf_info(path, pages_lim=25):
    '''
    extracts information from pdf file.

    Parameters
    ----------
    path : str
        path of pdf file.

    Returns
    -------
    dict
        dict with pdf info, such as creation date....

    '''
    stats = {}
    fname = Path(path).name
    hash1 = get_index_from_path(path)
    try:
        file = fitz.open(path)
        stats['pages'] = len(file)
        if stats['pages'] > pages_lim:
            stats['file'] = '#PAGES'
            return stats

        text = get_text_pdf(file)
        stats['file'] = 'OK'

        if len(text) < 100 * len(file):
            ocr(path, path, redo_ocr=True)
            file = fitz.open(path)
            text = get_text_pdf(file)

        if 'ocrmypdf' in file.metadata['creator']:
            stats['file'] = 'SCAN'

    except fitz.FileDataError:
        stats['file'] = 'ERR'
        return stats
    except ocrmypdf.EncryptedPdfError:
        stats['file'] = 'ENCRYPTED'
    except ocrmypdf.InputFileError:
        stats['file'] = 'FORM'
    try:
        date_creation_raw = file.metadata['creationDate']
    except Exception:
        date_creation_raw = ''

    strings = [date_creation_raw, fname, text]
    names = ['creation_date', 'fname', 'contents']
    stats.update(get_date(strings, names))
    distr_LAU = get_from_hash(hash1, 'c.distr, i.LAU')
    for page in file:
        for i, distr, LAU in distr_LAU.itertuples():
            quads = page.search_for(distr, quads=True)
            page.add_highlight_annot(quads)
    stats.update(get_content_stats(path, text))
    try:
        file.save(path, incremental=True)
        file.close()
    except RuntimeError:
        temp_path = os.path.join(hash1, f'temp_{fname}.pdf')
        file.save(temp_path)
        file.close()
        os.remove(path)
        os.rename(temp_path, path)
    except ValueError:
        print('Failed to add highlights.')

    return stats


def extract_excel_info(path):
    stats = {}
    try:
        tabs = pd.read_excel(path, sheet_name=None)
        stats['file'] = 'OK'
    except Exception:
        stats['file'] = 'ERR'
        return stats
    stats['pages'] = len(tabs)
    text = get_text_dfs(tabs)
    stats.update(get_content_stats(path, text))
    stats.update(get_date([text], ['content']))
    return stats


def get_text_dfs(df_dict):
    text = ''
    for i, df in df_dict.items():
        text += df.apply(lambda x: x.astype(str).add(' ').sum()
                         ).astype(str).add(' ').sum()
    return text


def get_text_pdf(pdf):
    text = ''
    for page in pdf:
        text += page.get_text()
    return re.sub('\n|\s+', ' ', text)


def find_year(text, pat=r'(201\d|202[012])'):
    find = re.findall(pat, text)
    if len(find) > 0:
        return max([int(y) for y in find])
    else:
        return


def get_date(strings, names):
    years = {}
    for string, name in zip(strings, names):
        year = find_year(string)
        if year:
            years[name] = year
    if 'fname' in years:
        data_status = years['fname']
        src = 'fname'
    elif 'creation_date' in years:
        data_status = years['creation_date']
        src = 'creation_date'
    elif 'content' in years:
        data_status = years['content']
        src = 'content'
    else:
        data_status = np.nan
        src = 'ERR'
    return {'date': data_status,
            'date_orig': src}


def update_file_info(query, n_0=0, n_1=-1, overwrite=False):
    os.chdir(query)
    db = open_db()

    if T.ind not in db.tables:
        update_file_index(query)

    if overwrite and T.info in db.tables:
        db.conn.execute('DROP TABLE {T.info}')

    if T.info not in db.tables:
        pd.DataFrame(columns=COLS_INFO).to_sql(
            T.info, db.conn, index_label='hash2')

    sql = f'SELECT ind.hash2, ind.hash, ind.fname, ind.ext \
        FROM {T.ind} ind \
        LEFT JOIN {T.info} info \
            ON ind.hash2 = info.hash2 \
        WHERE info.hash2 IS NULL'

    index = pd.read_sql(sql, db.conn)
    # return index

    for i, row in enumerate(index[n_0: n_1].itertuples()):
        stats = {key: np.nan for key in COLS_INFO}
        ind, hash2, hash1, fname, ext = row
        path = path_from_index([hash1, fname], ext)
        print(f'{i+n_0+1}/{index.index.size}: {path}')
        stats.update(get_base_stats(path))
        if stats['file'] != 'OMITTED':
            if ext == '.pdf':
                stats.update(extract_pdf_info(path))
            if ext in ['.xls', '.xlsx']:
                stats.update(extract_excel_info(path))
            if ext in ['.png', '.jpeg']:
                stats.update({'file': 'IMG'})

        data = pd.DataFrame.from_dict(
            stats, columns=[hash2], orient='index').T
        data.to_sql(T.info, db.conn, index_label='hash2',
                    if_exists='append')

        clear_output(wait=True)
    db.conn.close()
    os.chdir('..')
    return


def get_base_stats(path):
    stats = {}
    fname = Path(path).name
    blacklist = re.search(PATS['fname_blacklist'], fname, flags=re.IGNORECASE)
    ana = re.search(PATS['fname'] +
                    PATS['fname_omp'], fname, flags=re.IGNORECASE)
    omp = re.search(PATS['fname_omp'], fname, flags=re.IGNORECASE)
    if not blacklist:
        if ana:
            stats['analysis'] = 'YES'
        if omp:
            stats['OMP'] = 'YES'
    else:
        stats['file'] = 'OMITTED'
        return stats

    size = os.path.getsize(path)/1e6
    stats['MB'] = size
    return stats


def get_content_stats(path, text):
    stats = {}
    limit = re.search(PATS['limit'], text,
                      flags=re.DOTALL | re.IGNORECASE)
    data = re.findall(PATS['params'], text,
                      flags=re.DOTALL | re.IGNORECASE)

    calc_raw = re.search(PATS['median']+PATS['mean'], text,
                         flags=re.DOTALL | re.IGNORECASE)
    if calc_raw:
        calc = re.sub(PATS['median'], 'Median',
                      calc_raw.group(0), re.IGNORECASE)
        calc = re.sub(PATS['mean'], 'Mittelwert',
                      calc_raw.group(0), re.IGNORECASE)
    else:
        calc = np.nan
    hash1 = get_index_from_path(path)
    distr_LAU = get_from_hash(hash1, 'c.distr, i.LAU')
    mapping = {}
    for i, distr, LAU in distr_LAU.itertuples():
        pat = re.sub('\s[(].*[)]', '', distr)
        found_distr = re.search(
            pat, text+path, flags=re.IGNORECASE)
        if found_distr:
            mapping[distr] = LAU

    stats['data_basis'] = calc
    stats['legal_lim'] = 'YES' if limit else np.nan
    stats['params'] = len(data) if data else np.nan
    stats['distr'] = ', '.join(mapping.keys())
    stats['LAUs'] = ', '.join(str(LAU) for LAU in mapping.values())
    return stats
