# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 10:09:36 2022

@author: Leon
"""

import os
import random
import re
import shutil as sh
import time
import winsound
from datetime import datetime
from typing import Literal
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from IPython.display import clear_output
from lxml.etree import ParserError
from requests.exceptions import ConnectionError, ReadTimeout, SSLError
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from tqdm import tqdm
from urllib3.exceptions import HTTPError, MaxRetryError

from .complements import PATS, SERVICE, TIMEOUT
from .status import Status
from .converter import orient_data
from .database import LAU_NUTS, WVG, WVG_LAU, File_Index, Response, Supplier
from .demog_data import get_districts_from_comm
from .paths import PATH_DATA
from .utils import crop_text, hashf

scrape_err = (HTTPError, ConnectionError, SSLError, ReadTimeout, MaxRetryError)


def get_page(link):
    """
    Opens page and returns contents as BS object.

    Parameters
    ----------
    link : str
        URL of page.

    Returns
    -------
    BS object
        HTML-text of page.

    """
    try:
        resp = requests.get(link, timeout=TIMEOUT)
        try:
            soup = bs(resp.text, features="lxml")
            return soup
        except bs.HTMLParser.HTMLParseError:
            pass
        finally:
            resp.close()
    except scrape_err:
        pass


def save_tables(soup: bs, link: str, hash1: str):
    """
    Searches page for HTML tables and
    saves them to a single Excel file with multiple sheets.

    Parameters
    ----------
    soup : BS object
        HTML-text of page.
    comm : str
        name of community.

    Returns
    -------
    dict
        number of found and downloaded HTML tables.

    """
    try:
        trans = str.maketrans({".": "", ",": "."})
        pat = re.compile(PATS["DATA"], flags=re.IGNORECASE)
        tabs = pd.read_html(str(soup).translate(trans), match=pat)
    except ParserError:
        return {"n_tables": -1}, []
    except ValueError:
        return {"n_tables": 0}, []

    tabs = [orient_data(tab) for tab in tabs if orient_data(tab) is not None]
    if len(tabs) == 0:
        return {"n_tables": 0}
    pat_d = r"(?:\d{1,2})?\.?"
    pat_m = r"(?:[A-Z]\w|\d{1,2})?\.?"
    pat_y = r"(20(?:[01]\d|2[012]))"
    pat = rf'(?:{PATS["DATA_STATUS"]}).*' + pat_d + pat_m + pat_y

    finds = soup.find_all(string=re.compile(pat, flags=re.IGNORECASE))
    years = [
        re.search(pat_y, find).group(0) for find in finds if re.search(pat_y, find)
    ]
    save_HTML_tabs(tabs, "TAB")
    fname = "TAB_HTML"
    ext = ".xlsx"
    tables = [
        File_Index(
            hash=hash1,
            fname=fname,
            ext=ext,
            url=link,
            hash2=hashf(f"{hash1}/{fname}.{ext}"),
        )
    ]
    return ({"years": ", ".join(list(set(years)))}, tables)


def save_pdf(soup: bs, b_href: str, hash1: str):
    """
    Searches page for downloadable PDF files and does so.

    Parameters
    ----------
    soup : BS object
        HTML-text of page.
    b_href : str
        base hyperref of page up to TLD.
    comm : str
        name of community.

    Returns
    -------
    dict
        number of found PDF, PDF errors and PDF names.

    """
    err = 0
    matches = soup.find_all(href=re.compile(".pdf"))
    hrefs = set(match["href"] for match in matches)
    pdfs = []
    fnames = []
    for i, href in enumerate(hrefs):
        fname = re.search(r"(?<=\w/)[\w-]+(?=\.pdf)", href)
        if fname:
            fname = fname.group(0)
        else:
            fname = f"PDF_{i}"
        if fname in fnames:
            continue
        fnames.append(fname)

        link = urljoin(b_href, href) if "http" not in href else href
        if re.search(PATS["BLACKLIST_FNAME"], link, flags=re.IGNORECASE):
            continue
        try:
            resp = requests.get(link, allow_redirects=True, timeout=TIMEOUT)
        except scrape_err:
            continue

        if resp.ok:
            open(f"{fname}.pdf", "wb").write(resp.content)
            pdfs.append(
                File_Index(
                    hash=hash1,
                    fname=fname,
                    ext=".pdf",
                    url=link,
                    hash2=hashf(f"{hash1}/{fname}.pdf"),
                )
            )
        else:
            err += 1
    return {"err_pdf": err}, pdfs


def save_other(soup: bs, b_href: str, hash1: str):
    """
    Searches page for downloadable IMG files and does so.

    Parameters
    ----------
    soup : BS object
        HTML-text of page.
    b_href : str
        base hyperref of page up to TLD.
    comm : str
        name of community.

    Returns
    -------
    dict
        number of found IMG, IMG errors and IMG names.

    """
    err = 0
    exts = [".jpeg", ".png", ".xlsx", ".xls", ".csv"]
    n_files = 0
    files = []
    for ext in exts:
        srcs = soup.find_all(src=re.compile(re.escape(ext)))
        hrefs = soup.find_all(href=re.compile(re.escape(ext)))
        srcs = [src["src"] for src in srcs]
        hrefs = [href["href"].strip('"‘’‚‛“”„‟') for href in hrefs]
        links = set(srcs + hrefs)

        fnames = []
        for i, link in enumerate(links):
            pat = f"[\w\d_-]+(?={re.escape(ext)})"
            find = re.search(pat, link)
            if find:
                fname = find.group(0)
            else:
                fname = f"FILE_{i}"
            if fname in fnames:
                continue
            fnames.append(fname)

            if re.search(PATS["BLACKLIST_FNAME"], link, flags=re.IGNORECASE):
                continue
            if "http" not in link:
                link = urljoin(b_href, link)
            try:
                file = requests.get(link, allow_redirects=True, timeout=TIMEOUT)
            except scrape_err:
                continue

            if file.ok:
                open(f"{fname}{ext}", "wb").write(file.content)
                files.append(
                    File_Index(
                        hash=hash1,
                        fname=fname,
                        ext=ext,
                        url=link,
                        hash2=hashf(f"{hash1}/{fname}.pdf"),
                    )
                )
            else:
                err += 1
        n_files += len(fnames)
    return {"err_other": err}, files


def scrape_page(i: int, link: str, LAU: int, comm: str, hash1: str):
    time = datetime.now()
    b_href = re.search(r"\A.+\.[a-z]+(?=/)", str(link)).group(0)
    inst = []
    req = Response(
        LAU=LAU,
        position=i,
        link=link,
        b_href=b_href,
        supplier=is_supplier(b_href, comm),
        time=time,
        status=Status.OMITTED,
        hash=hash1,
    )
    if re.match(PATS["BLACKLIST_URL"] + PATS["BLACKLIST_PRESS"], link):
        return req, inst

    # check if link leads to pdf and download it
    if re.search(r"\.pdf", link, re.IGNORECASE):
        try:
            page = requests.get(link, allow_redirects=True, timeout=TIMEOUT)
        except scrape_err:
            req.status = Status.ERROR
            return req, inst

        if page:
            req.status = Status.OK
        else:
            req.status = Status.ERROR
        fname = re.search(r"(?<=\w/)[\w-]+(?=\.)", link)
        if fname:
            fname = fname.group(0)
        else:
            fname = f"{comm}_{i}"
        open(fname + ".pdf", "wb").write(page.content)
        req.status = Status.DOWNLOAD

    else:
        page = get_page(link)
        if not page:
            req.status = Status.ERROR
            return req, inst

        req.status = Status.OK

        # scrape HTMl for downloads and save relevant info
        html_stat, tables = save_tables(page, link, hash1)
        img_stat, others = save_other(page, b_href, hash1)
        pdf_stat, pdfs = save_pdf(page, b_href, hash1)
        inst = tables + others + pdfs
        req.__dict__.update({**html_stat, **img_stat, **pdf_stat})

        if page.find(string=re.compile(PATS["ADDRESS"], flags=re.IGNORECASE)):
            postcode = Status.YES
        else:
            postcode = Status.NO
        districts = get_districts_from_comm(comm)
        pat_dist = "|".join(districts)
        finds = page.find_all(string=re.compile(pat_dist, flags=re.IGNORECASE))
        dist_finds = list(
            set(
                [
                    re.search(pat_dist, find).group(0)
                    for find in finds
                    if re.search(pat_dist, find)
                ]
            )
        )
        req.districts = ", ".join(dist_finds)
        req.postcode = postcode
    return req, inst


class google_webdriver:
    def __init__(self, link="https://www.google.de/"):
        self.link = link

    def get_links(self, query, n_res=-1):
        field = self.driver.find_element(By.CSS_SELECTOR, "input")
        try:
            WebDriverWait(self, timeout=10).until(EC.element_to_be_clickable(field))
        except TimeoutException:
            self.driver.maximize_window()
            winsound.Beep(3000, 500)
            input("Press any key to contniue.")
        field.clear()
        field.send_keys(query)
        field.submit()
        results = self.driver.find_elements(By.CLASS_NAME, "g")
        links = []
        for i, result in enumerate(results):
            try:
                pat = f"//*[@id='rso']/div[{i}]//a"
                links.append(result.find_element(By.XPATH, pat).get_attribute("href"))
            except Exception:
                pass
        return links[:n_res]

    def __enter__(self):
        self.driver = webdriver.Chrome(service=SERVICE)
        self.driver.minimize_window()
        self.driver.get(self.link)
        agreement = self.driver.find_element(By.CLASS_NAME, "KxvlWc")
        agreement.find_element(By.XPATH, '//*[@id="W0wltc"]').click()  # disagree
        return self

    def __exit__(self, *args):
        print(args)
        self.driver.close()


#


def check_comm(b_href, comm):
    words = re.findall(r"\w+", comm)
    words = [replace_umlaut(word.lower()) for word in words]
    if re.search("|".join(words), b_href):
        return True


def replace_umlaut(string):
    replace = str.maketrans(dict(zip(["ä", "ö", "ü", "ß"], ["ae", "oe", "ue", "ss"])))
    return string.translate(replace)


def is_supplier(b_href, comm):
    if re.sub("(http[s]?://)?(www\.)?", "", b_href) in PATS["SUPPLIER_LIST"]:
        return Status.CONFIRMED
    elif re.search(PATS["BLACKLIST_URL"], b_href):
        return Status.COMMERCIAL
    elif re.search(PATS["BLACKLIST_PRESS"], b_href):
        return Status.PRESS
    elif re.search(PATS["SUPPLIER"], b_href):
        return Status.PROBABLY
    elif check_comm(b_href, comm):
        return Status.COMMUNITY


def save_HTML_tabs(tables, j, path=""):
    with pd.ExcelWriter(os.path.join(path, f"{j}_HTML.xlsx")) as writer:
        for i, df in enumerate(tables):
            df.to_excel(writer, sheet_name=f"{i}")


def scrape_pages(
    query,
    session: Session,
    how: Literal["supplier", "LAU"],
    start=0,
    stop=None,
    n_res=1,
    wait=10,
):
    """
    performs google search of "comm + query",
    opens pages of n_res results for each search term and scans pages.
    If save==True, contents and search data are saved.

    Parameters
    ----------
    communities : pd.DataFrame
        list of community names.
    query : str, optional
        search query. The default is ''.
    n : int, optional
        number of consecutive searches. The default is None.
    start : int or str, optional
        start number or name of comm in communities. The default is None.
    stop : int or str, optional
        stop number or name of comm in communities. The default is None.
    n_res : int, optional
        number of results per search. The default is 1.
    save : bool, optional
        save search results. The default is True.
    pause : float, optional
        delay between searches. The default is 2.0.
    user_agent : str, optional
        user agent for identification. The default is None.

    Returns
    -------
    None.

    """

    with google_webdriver() as driver:
        if not os.path.exists(PATH_DATA):
            os.mkdir(PATH_DATA)
        os.chdir(PATH_DATA)

        if how == "supplier":
            max_id = stop if stop else session.execute(func.max(Supplier.id)).scalar()
            stmt = (
                select(Supplier.name, Supplier.url)
                .outerjoin(Response)
                .where(Response.LAU == None, Supplier.id.between(start, max_id))
            )

        elif how == "LAU":
            max_id = stop if stop else session.execute(func.max(WVG_LAU.id)).scalar()
            stmt = (
                select(LAU_NUTS.name, WVG_LAU.LAU)
                .outerjoin(Response)
                .where(Response.LAU == None, WVG_LAU.id.between(start, max_id))
                .join(WVG)
                .join(LAU_NUTS)
                .order_by(WVG.supplied.desc())
            )

        else:
            return

        for name, var in (pbar := tqdm(session.execute(stmt).all(), desc="[ INFO  ]")):
            pbar.set_postfix_str(crop_text(name, 20))
            inst = []

            if how == "LAU":
                name = re.sub(r"\s?[,/].*\Z", "", name)
                search_string = f"{name} {query}"
            elif how == "supplier":
                search_string = f"site:{var} {query}"

            t_0 = time.time()
            res = driver.get_links(search_string, n_res)
            for i, link in enumerate(res):
                hash1 = hashf(link)
                dir_page = f"{hash1}"
                stmt = select(Response).where(Response.hash == hash1)
                if session.execute(stmt).first():
                    continue
                if os.path.exists(dir_page):
                    sh.rmtree(dir_page)
                os.mkdir(dir_page)
                os.chdir(dir_page)

                if how == "LAU":
                    args = (i, link, var, name, hash1)
                elif how == "supplier":
                    args = (i, link, None, "", hash1)
                req, files = scrape_page(*args)

                inst.append(req)
                inst.extend(files)
                os.chdir("..")
                if os.listdir(dir_page) == []:
                    os.rmdir(dir_page)
                    continue

                session.add_all(inst)
                session.commit()
            clear_output(wait=True)
            t_1 = time.time()
            dt = t_1 - t_0
            if (diff := wait - dt) > 0:
                sleep_for = diff + random.uniform(0, wait / 2)
                time.sleep(sleep_for)
        os.chdir("..")

    return


# def scrape_pages_LAU(
#     query,
#     session: Session,
#     start=0,
#     stop=None,
#     n_res=1,
# ):
#     """
#     performs google search of "comm + query",
#     opens pages of n_res results for each search term and scans pages.
#     If save==True, contents and search data are saved.

#     Parameters
#     ----------
#     communities : pd.DataFrame
#         list of community names.
#     query : str, optional
#         search query. The default is ''.
#     n : int, optional
#         number of consecutive searches. The default is None.
#     start : int or str, optional
#         start number or name of comm in communities. The default is None.
#     stop : int or str, optional
#         stop number or name of comm in communities. The default is None.
#     n_res : int, optional
#         number of results per search. The default is 1.
#     save : bool, optional
#         save search results. The default is True.
#     pause : float, optional
#         delay between searches. The default is 2.0.
#     user_agent : str, optional
#         user agent for identification. The default is None.

#     Returns
#     -------
#     None.

#     """

#     with google_webdriver() as driver:
#         if not os.path.exists(PATH_DATA):
#             os.mkdir(PATH_DATA)
#         os.chdir(PATH_DATA)
#         max_id = stop if stop else session.execute(func.max(WVG_LAU.id)).scalar()
#         stmt = (
#             select(LAU_NUTS.name, WVG_LAU.LAU)
#             .outerjoin(Response)
#             .where(Response.LAU == None, WVG_LAU.id.between(start, max_id))
#             .join(WVG)
#             .join(LAU_NUTS)
#         )
#         for name, LAU in session.execute(stmt):
#             inst = []
#             name = re.sub(r"\s?[,/].*\Z", "", name)
#             search_string = f"{name} {query}"
#             res = driver.get_links(search_string, n_res)
#             for i, link in enumerate(res):
#                 hash1 = hashf(link)
#                 dir_page = f"{hash1}"
#                 stmt = select(Response).where(Response.hash == hash1)
#                 if session.execute(stmt).first():
#                     continue
#                 if os.path.exists(dir_page):
#                     sh.rmtree(dir_page)
#                 os.mkdir(dir_page)
#                 os.chdir(dir_page)

#                 req, files = scrape_page(i, link, LAU, name, hash1)
#                 inst.append(req)
#                 inst.extend(files)
#                 os.chdir("..")
#                 if os.listdir(dir_page) == []:
#                     os.rmdir(dir_page)
#                     continue

#             session.add_all(inst)
#             session.commit()
#             clear_output(wait=True)
#         os.chdir("..")

#     return


# def manual_scrape(query):
#     os.chdir(query)
#     op = webdriver.ChromeOptions()
#     path = fr'G:\UBA_WIMI\1_TriSto\4_Skript\{query}\download'
#     if not os.path.exists(path):
#         os.mkdir(path)
#     p = {'download.default_directory': path}
#     op.add_experimental_option('prefs', p)
#     driver = webdriver.Chrome(options=op, service=SERVICE)
#     sel = 'community, link'
#     on = 's.LAU = l.LAU'
#     where = 'supplier="YES" AND clicks IS NULL'
#     having = '(SUM(n_pdf) = 0 AND SUM(n_tables) = 0)'
#     sql = f'SELECT {sel} FROM index_scrape s JOIN LAU_comm l ON {on} WHERE {where} GROUP BY s.LAU HAVING {having}'
#     to_update = db.read(sql)
#     # db.read(sql).drop_duplicates(subset='link').itertuples()
#     for i, tup in enumerate(to_update.itertuples()):
#         i, comm, link = tup
#         driver.get(link)
#         link_0 = driver.current_url
#         print(f'{i+1}/{to_update.index.size}: {comm}; {link}')
#         ch = input('Is supplier?')
#         url = driver.current_url
#         hash1 = hashf(url)

#         if ch == 'q':
#             break
#         elif ch in ['0', 'n']:
#             db.exe(
#                 f'UPDATE index_scrape SET supplier="NO" WHERE link="{link}"')
#             continue
#         elif ch in ['1', 'y', 'html']:
#             if ch == 'html':
#                 handles = driver.window_handles
#                 n_tabs = len(handles)
#                 original_window = driver.current_window_handle
#                 for j, tab in enumerate(handles):
#                     driver.switch_to.window(tab)
#                     try:
#                         tables = pd.read_html(
#                             driver.page_source.replace(',', '.'))
#                         save_HTML_tabs(tables, path, j)
#                     except ValueError:
#                         pass

#                 db.exe(
#                     f'UPDATE index_scrape SET n_tables={n_tabs} WHERE link="{link}"')
#                 driver.switch_to.window(original_window)

#         num = 0
#         while driver.current_url != link_0:
#             driver.back()
#             num += 1

#         files = os.listdir(path)

#         if files:
#             if not os.path.exists(hash1):
#                 os.mkdir(hash1)

#             for file in files:
#                 src = os.path.join(path, file)
#                 dst = os.path.join(hash1, file)
#                 sh.move(src, dst)

#             files = os.listdir(hash1)
#             n_pdf = len([file for file in files if file.endswith('.pdf')])
#             n_tables = len([file for file in files if file.endswith('.xlsx')])
#         else:
#             n_pdf = n_tables = 0
#         s = f'supplier="YES", clicks="{num}", hash="{hash1}", link="{url}", init_link="{link}", n_pdf="{n_pdf}", n_tables="{n_tables}"'
#         where = f'link="{link}"'
#         db.exe(f'UPDATE index_scrape SET {s} WHERE {where}')

#         clear_output(wait=False)
#     os.chdir('..')


# def scrape_pages_Supplier(query, session: Session, n_res=1, wait=10):
#     """
#     performs google search of "comm + query",
#     opens pages of n_res results for each search term and scans pages.
#     If save==True, contents and search data are saved.

#     Parameters
#     ----------
#     communities : pd.DataFrame
#         list of community names.
#     query : str, optional
#         search query. The default is ''.
#     n : int, optional
#         number of consecutive searches. The default is None.
#     start : int or str, optional
#         start number or name of comm in communities. The default is None.
#     stop : int or str, optional
#         stop number or name of comm in communities. The default is None.
#     n_res : int, optional
#         number of results per search. The default is 1.
#     save : bool, optional
#         save search results. The default is True.
#     pause : float, optional
#         delay between searches. The default is 2.0.
#     user_agent : str, optional
#         user agent for identification. The default is None.

#     Returns
#     -------
#     None.

#     """

#     with google_webdriver() as driver:
#         os.chdir(HOME)
#         if not os.path.exists(PATH_DATA):
#             os.mkdir(PATH_DATA)
#         os.chdir(PATH_DATA)

#         stmt = select(Supplier.name, Supplier.url)
#         for i, (name, url) in enumerate(session.execute(stmt)):
#             statement = select(Supplier).where(Supplier.url == url)
#             if session.execute(statement).first():
#                 continue
#             n_suppliers = session.execute(func.count(Supplier.url != None)).scalar()
#             print(f'{i} of {n_suppliers}: "{name}", "{url}"')
#             search_string = f"site:{url} {query}"
#             res = driver.get_links(search_string, n_res)
#             t_0 = time.time()
#             session.add(Supplier(name=name, url=url))
#             for j, link in enumerate(res):
#                 hash1 = hashf(link)
#                 dir_page = f"{hash1}"
#                 statement = select(Response).where(Response.hash == hash1)
#                 if session.execute(statement).first():
#                     continue
#                 if os.path.exists(dir_page):
#                     sh.rmtree(dir_page)
#                 os.mkdir(dir_page)
#                 os.chdir(dir_page)

#                 req, files = scrape_page(j, link, None, "", hash1)
#                 session.add(req)
#                 session.add_all(files)
#                 os.chdir("..")
#                 try:
#                     os.rmdir(dir_page)
#                     continue
#                 except OSError:
#                     pass

#             session.commit()
#             clear_output(wait=True)
#             t_1 = time.time()
#             dt = t_1 - t_0
#             if (diff := wait - dt) > 0:
#                 sleep_for = diff + random.uniform(0, wait / 2)
#                 time.sleep(sleep_for)
#         os.chdir("..")

#     return
