# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 10:08:54 2022

@author: Leon
"""
import os
import re
from pathlib import Path

import camelot as cm
import fitz
import ocrmypdf
import pandas as pd
from IPython.display import clear_output
from ocrmypdf import ocr
from openpyxl.utils.exceptions import IllegalCharacterError
from PIL.Image import DecompressionBombError
from sqlalchemy import delete, select
from sqlalchemy.orm import Session
from tabula import read_pdf
from tqdm import tqdm

from .cleaner import clean_data
from .complements import PATS, Status
from .database import File_Cleaned, File_Index, File_Info, OpenDB
from .index_utils import get_index_from_path, path_from_index
from .paths import HOME, PATH_CONV, PATH_DATA
from .utils import count_occ


def extract_pdf(file, hash2, save=True, overwrite=False):
    """
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
        overwritePATH_CONVfiles. The default is False.

    Returns
    -------
    dict
        dict conatining converter and conversion rate.

    """
    fname, _ = os.path.splitext(file)
    stat = {}
    if os.path.exists(fname + ".xlsx") and overwrite is False:
        return

    with fitz.open(file) as doc:
        text = get_text_pdf(doc)

    matches = re.findall(PATS["RAW_PARAMS"], re.sub("\n", " ", text))
    n_matches = len(matches)

    extracted_tables = {"tables": {}, "n_params": {}, "cells": {}}
    try:
        tables = cm.read_pdf(
            file, pages="all", flavor="stream", edge_tol=500, row_tol=5
        )

        tables = [table.df for table in tables]

        n_params = count_occ(tables, PATS["PARAMS"])
        extracted_tables["tables"][Status.CAMELOT] = tables
        extracted_tables["n_params"][Status.CAMELOT] = n_params
        extracted_tables["cells"][Status.CAMELOT] = sum(
            [table.size for table in tables]
        )
        if n_params < n_matches:

            raise Exception

    except Exception:
        try:
            tables = read_pdf(file, pages="all")
            tables = [
                table.dropna(how="all").dropna(axis=1, how="all")
                for table in tables
                if table.notna().to_numpy().any()
            ]
            n_params = count_occ(tables, PATS["PARAMS"])
            extracted_tables["tables"][Status.TABULA] = tables
            extracted_tables["n_params"][Status.TABULA] = n_params
            extracted_tables["cells"][Status.TABULA] = sum(
                [table.size for table in tables]
            )

        except Exception:
            return {"converter": Status.ERROR}

    converter = max(extracted_tables["cells"], key=extracted_tables["cells"].get)
    tables = extracted_tables["tables"][converter]
    n_params = extracted_tables["n_params"][converter]
    news = []
    err = 0
    n_tab = len(tables)
    if n_tab == 0:
        return {"converter": Status.ERROR}

    for table in tables:
        try:
            new = clean_data(table)
            news += [new]
        except Exception:
            err += 1
            continue

    if save and news != []:
        dst = PATH_CONV / f"{hash2}.xlsx"
        save_tables(news, dst)
    stat = {
        "converter": converter,
        "tabs_dropped": err,
        "tabs_total": n_tab,
        "n_params": n_params,
    }
    return stat


def extract_excel(file, hash2):
    tables = pd.read_excel(file, sheet_name=None)
    tables = [table for table in tables.values()]
    n_params = count_occ(tables, PATS["PARAMS"])

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
        dst = PATH_CONV / f"{hash2}.xlsx"
        save_tables(news, dst)
    stats = {
        "converter": Status.NONE,
        "tabs_dropped": err,
        "tabs_total": n_tab,
        "n_params": n_params,
    }

    return stats


def extract_tables(session: Session, min_params=10, overwrite=False):
    os.chdir(PATH_DATA)

    statement = (
        select(
            File_Index.hash,
            File_Index.fname,
            File_Index.ext,
            File_Index.hash2,
            File_Cleaned.hash2,
            File_Info.MB,
            File_Info.pages,
        )
        .outerjoin(File_Info)
        .outerjoin(File_Cleaned)
        .filter(
            File_Info.status.in_((Status.OK, Status.SCAN)),
            File_Info.n_param > min_params,
            File_Index.ext.in_((".pdf", ".xlsx", ".xls")),
        )
    )
    for i, (hash1, fname, ext, hash2, fc_hash, MB, pages) in enumerate(
        result := session.execute(statement).all()
    ):
        if overwrite:
            stmt = delete(File_Cleaned).where(File_Cleaned.hash2 == hash2)
            session.execute(stmt)
            session.commit()
        else:
            if fc_hash:
                continue

        fpath = f"{hash1}/{fname}{ext}"
        print(f"{i+1} of {len(result)}: {fpath!r}")
        stat = {}
        os.chdir(hash1)
        file = f"{fname}{ext}"
        try:
            if ext == ".pdf":
                if MB / pages > 2:
                    stat.update({"status": Status.DENSITY})
                else:
                    stat.update(extract_pdf(file, hash2, overwrite=overwrite))

            elif ext in [".xls", ".xlsx"]:
                stat.update(extract_excel(file, hash2))

        except:
            stat.update({"status": Status.ERROR})

        session.add(File_Cleaned(hash2=hash2, **stat))
        session.commit()
        clear_output(wait=True)
        os.chdir("..")
    os.chdir(HOME)
    return


def to_list(df):
    """
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

    """
    merge = []
    for col in df.columns:
        temp = df.loc[:, col].dropna(axis=0)
        split = pd.DataFrame(temp.to_list(), index=temp.index)
        merge += [split]
    return pd.concat(merge, axis=1, ignore_index=True)


def save_tables(tables, fname):
    """
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

    """
    with pd.ExcelWriter(fname) as writer:
        for i, table in enumerate(tables):
            try:
                table.to_excel(writer, sheet_name=str(i))
            except IllegalCharacterError:
                table.applymap(
                    lambda x: x.encode("unicode_escape").decode("utf-8")
                    if isinstance(x, str)
                    else x
                ).to_excel(writer, sheet_name=str(i))


def removesuffix(string, suffix):
    if string.endswith(suffix):
        return string[: -len(suffix)]
    else:
        return string


def extract_pdf_info(path, pages_lim=25):
    """
    extracts information from pdf file.

    Parameters
    ----------
    path : str
        path of pdf file.

    Returns
    -------
    dict
        dict with pdf info, such as creation date....

    """
    stats = {}
    fname = Path(path).name
    hash1 = get_index_from_path(path)
    try:
        file = fitz.open(path)
        stats["pages"] = len(file)
        if stats["pages"] > pages_lim:
            stats["status"] = "#PAGES"
            return stats

        text = get_text_pdf(file)
        stats["status"] = "OK"

        if "ocrmypdf" in file.metadata["creator"]:
            stats["status"] = "SCAN"
        elif len(text) < 100 * len(file):
            ocr(path, path, redo_ocr=True, max_image_mpixels=10)
            file = fitz.open(path)
            text = get_text_pdf(file)

    except fitz.FileDataError:
        stats["status"] = Status.ERROR
        return stats
    except DecompressionBombError:
        stats["status"] = Status.DECOMP
        return stats
    except ocrmypdf.EncryptedPdfError:
        stats["status"] = Status.ENCRYPTED
    except ocrmypdf.InputFileError:
        stats["status"] = Status.FORM
    try:
        date_creation_raw = file.metadata["creationDate"]
    except Exception:
        date_creation_raw = ""

    strings = [date_creation_raw, fname, text]
    names = ["creation_date", "fname", "contents"]
    stats.update(get_date(strings, names))
    # distr_LAU = get_from_hash(hash1, "c.distr, i.LAU")
    # for page in file:
    #     for i, distr, LAU in distr_LAU.itertuples():
    #         quads = page.search_for(distr, quads=True)
    #         page.add_highlight_annot(quads)
    stats.update(get_content_stats(path, text))
    try:
        file.save(path, incremental=True)
        file.close()
    except RuntimeError:
        temp_path = os.path.join(hash1, f"temp_{fname}.pdf")
        file.save(temp_path)
        file.close()
        os.remove(path)
        os.rename(temp_path, path)
    except ValueError:
        print("Failed to add highlights.")

    return stats


def extract_excel_info(path):
    stats = {}
    try:
        tabs = pd.read_excel(path, sheet_name=None)
        stats["status"] = Status.OK
    except Exception:
        stats["status"] = Status.ERROR
        return stats
    stats["pages"] = len(tabs)
    text = get_text_dfs(tabs)
    stats.update(get_content_stats(path, text))
    stats.update(get_date([text], ["content"]))
    return stats


def get_text_dfs(df_dict):
    text = ""
    for df in df_dict.values():
        text += (
            df.apply(lambda x: x.astype(str).add(" ").sum()).astype(str).add(" ").sum()
        )
    return text


def get_text_pdf(pdf):
    text = ""
    for page in pdf:
        text += page.get_text()
    return re.sub("\n|\s+", " ", text)


def find_year(text, pat=r"(201\d|202[012])"):
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
    if "fname" in years:
        data_status = years["fname"]
        src = Status.FNAME
    elif "creation_date" in years:
        data_status = years["creation_date"]
        src = Status.CREATION_DATE
    elif "content" in years:
        data_status = years["content"]
        src = Status.CONTENT
    else:
        data_status = None
        src = Status.ERROR
    return {"date": data_status, "date_orig": src}


def update_file_info(session: Session):
    os.chdir(PATH_DATA)
    statement = select(
        File_Index.hash, File_Index.fname, File_Index.ext, File_Index.hash2
    )
    skipped = 0
    for i, (hash1, fname, ext, hash2) in enumerate(
        result := session.execute(statement).all()
    ):
        statement = select(File_Info).where(File_Info.hash2 == hash2)
        if session.execute(statement).first():
            skipped += 1
            continue

        fpath = f"{hash1}/{fname}{ext}"
        print(f"{i+1} of {len(result)}: {fpath!r}")
        path = path_from_index([hash1, fname], ext)
        stats = get_base_stats(path)
        if stats["status"] != Status.OMITTED:
            if ext == ".pdf":
                stats.update(extract_pdf_info(path))
            if ext in [".xls", ".xlsx"]:
                stats.update(extract_excel_info(path))
            if ext in [".png", ".jpeg"]:
                stats.update({"status": Status.IMG})
        session.add(File_Info(hash2=hash2, **stats))
        session.commit()
        clear_output(wait=True)
    os.chdir(HOME)
    print(f"Update complete. Skipped {skipped} files.")
    return


def get_base_stats(path):
    stats = {}
    fname = Path(path).name
    size = os.path.getsize(path) / 1e6
    stats["MB"] = size
    blacklist = re.search(PATS["BLACKLIST_FNAME"], fname, flags=re.I)
    ana = re.search(PATS["FNAME"] + PATS["FNAME_OMP"], fname, flags=re.I)
    omp = re.search(PATS["FNAME_OMP"], fname, flags=re.I)
    if not blacklist:
        if ana:
            stats["analysis"] = Status.YES
        if omp:
            stats["OMP"] = Status.YES
        stats["status"] = Status.OK
    else:
        stats["status"] = Status.OMITTED
    return stats


def get_content_stats(path, text):
    stats = {}
    data = re.findall(PATS["RAW_PARAMS"], text, flags=re.S | re.I)

    calc_raw = re.search(PATS["MEDIAN"] + PATS["MEAN"], text, flags=re.S | re.I)
    if calc_raw:
        calc = re.sub(PATS["MEDIAN"], Status.MEDIAN, calc_raw.group(0), re.I)
        calc = re.sub(PATS["MEAN"], Status.AV, calc_raw.group(0), re.I)
    else:
        calc = None
    mapping = {}

    # hash1 = get_index_from_path(path)
    # distr_LAU = get_from_hash(hash1, "c.distr, i.LAU")

    # for i, distr, LAU in distr_LAU.itertuples():
    #     pat = re.sub("\s[(].*[)]", "", distr)
    #     found_distr = re.search(pat, text + path, flags=re.I)
    #     if found_distr:
    #         mapping[distr] = LAU

    stats["data_basis"] = calc
    stats["n_param"] = len(data) if data else None
    stats["districts"] = ", ".join(mapping.keys())
    stats["LAUS"] = ", ".join(str(LAU) for LAU in mapping.values())
    return stats
