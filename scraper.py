# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 10:09:36 2022

@author: Leon
"""

from dataclasses import dataclass
from urllib.error import HTTPError
from bs4 import BeautifulSoup as bs
from .complements import TIMEOUT, HOME, SERVICE, hashf, PATS
from .converter import clean_data
from .database import WVG, Response, WVG_LAU, File_Index
from datetime import datetime
from .demog_data import get_from_LAU, get_districts_from_comm, WVG_list
from IPython.display import clear_output
import os
import pandas as pd
import requests
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
import shutil as sh
from urllib.parse import urljoin, urlparse
from sqlmodel import SQLModel, create_engine, Session, select
from pathlib import Path


@dataclass(frozen=True)
class Status:
    OK: str = "OK"
    ERROR: str = "ERROR"
    OMITTED: str = "OMITTED"
    DOWNLOAD: str = "DOWNLOAD"

    YES: str = "YES"
    NO: str = "NO"
    CONFIRMED: str = "CONFIRMED"

    IMG: str = "IMG"
    SCAN: str = "SCAN"
    N_PAGES: str = "N_PAGES"

    COMMERCIAL: str = "COMMERCIAL"
    PRESS: str = "PRESS"
    COMMUNITY: str = "COMMUNITY"
    PROBABLY: str = "PROBABLY"


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
    except HTTPError:
        pass


def save_tables(soup):
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
    n_tables = "tables"

    try:
        trans = str.maketrans({".": "", ",": "."})
        pat = re.compile(PATS["data"], flags=re.IGNORECASE)
        tabs = pd.read_html(str(soup).translate(trans), match=pat)
    except ValueError:
        return {n_tables: 0}
    print(tabs)
    tabs = [clean_data(tab) for tab in tabs if clean_data(tab) is not None]
    if len(tabs) == 0:
        return {n_tables: 0}
    pat_d = r"(?:\d{1,2})?\.?"
    pat_m = r"(?:[A-Z]\w|\d{1,2})?\.?"
    pat_y = r"(20(?:[01]\d|2[012]))"
    pat = rf'(?:{PATS["data_status"]}).*' + pat_d + pat_m + pat_y

    finds = soup.find_all(string=re.compile(pat, flags=re.IGNORECASE))
    years = [
        re.search(pat_y, find).group(0) for find in finds if re.search(pat_y, find)
    ]
    save_HTML_tabs(tabs, "TAB")
    return {n_tables: len(tabs), "years": ", ".join(list(set(years)))}


def save_pdf(soup, b_href):
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
    files = soup.find_all(href=re.compile(".pdf"))
    names = ["pdf", "err_pdf"]
    for i, file in enumerate(files):
        href = file["href"]
        link = urljoin(b_href, href) if "http" not in href else href
        if re.search(PATS["fname_blacklist"], link, flags=re.IGNORECASE):
            continue
        try:
            resp = requests.get(link, allow_redirects=True, timeout=TIMEOUT)
        except HTTPError:
            continue

        fname = re.search(r"(?<=\w/)[\w-]+(?=\.pdf)", href)
        if fname:
            fname = fname.group(0)
        else:
            fname = f"PDF_{i}"
        if resp.ok:
            open(fname + ".pdf", "wb").write(resp.content)
        else:
            err += 1
    return dict(zip(names, [len(files), err]))


def save_other(soup, b_href):
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
    names = ["other", "err_other"]
    n_files = 0
    for ext in exts:
        srcs = soup.find_all(src=re.compile(re.escape(ext)))
        hrefs = soup.find_all(href=re.compile(re.escape(ext)))
        srcs = [src["src"] for src in srcs]
        hrefs = [href["href"].strip('"‘’‚‛“”„‟') for href in hrefs]
        links = srcs + hrefs
        n_files += len(links)
        for i, link in enumerate(links):
            if re.search(PATS["fname_blacklist"], link, flags=re.IGNORECASE):
                continue
            if "http" not in link:
                link = urljoin(b_href, link)
            try:
                file = requests.get(link, allow_redirects=True, timeout=TIMEOUT)
            except HTTPError:
                continue
            fname = Path(link).name
            if not fname:
                fname = f"FILE_{i}"
            if file.ok:
                open(fname, "wb").write(file.content)
            else:
                err += 1
    return dict(zip(names, [n_files, err]))


def scrape_page(i, link, LAU, comm, hash1):
    time = datetime.now()
    b_href = re.search(r"\A.+\.[a-z]+(?=/)", str(link)).group(0)
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
    if re.match(PATS["blacklist"] + PATS["blacklist_press"], link):
        return req

    # check if link leads to pdf and download it
    if re.search(r"\.pdf", link, re.IGNORECASE):
        page = requests.get(link, allow_redirects=True, timeout=TIMEOUT)
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
        if page:
            req.status = Status.OK

            # scrape HTMl for downloads and save relevant info
            html_stat = save_tables(page)
            img_stat = save_other(page, b_href)
            pdf_stat = save_pdf(page, b_href)
            req.__dict__.update({**html_stat, **img_stat, **pdf_stat})

            if page.find(string=re.compile(PATS["address"], flags=re.IGNORECASE)):
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
        else:
            req.status = Status.ERROR
    return req


class google_webdriver:
    def __init__(self, link="https://www.google.de/"):
        self.link = link

    def get_links(self, query, n_res=-1):
        field = self.driver.find_element(By.CSS_SELECTOR, '[name="q"]')
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
        button = self.driver.find_element(By.CLASS_NAME, "VDity")
        button.find_element(By.XPATH, '//*[@id="L2AGLb"]').click()
        return self

    def __exit__(self, *args):
        print(args)
        self.driver.close()


def scrape_pages_LAU(query="", start=0, stop=-1, n_res=1):
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
    os.chdir(HOME)
    if not os.path.exists(query):
        os.mkdir(query)
    os.chdir(query)

    engine = create_engine(f"sqlite:///{query}.db")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session, google_webdriver() as driver:
        for id, name, LAUs, supplied, discharge in WVG_list[start:stop].itertuples():
            inst = []
            statement = select(WVG).where(WVG.id == id)
            if session.exec(statement).first():
                continue
            inst.append(WVG(id=id, name=name, supplied=supplied, discharge=discharge))
            LAU_list = str(LAUs).split(", ")
            for LAU in LAU_list:
                LAU_dict = get_from_LAU(LAU)
                name = re.sub(r"\s?[,/].*\Z", "", LAU_dict["full_name"])
                inst.append(WVG_LAU(id=id, name=name, **LAU_dict))
                search_string = f"{name} {query}"
                res = driver.get_links(search_string, n_res)
                for i, link in enumerate(res):
                    hash1 = hashf(link)
                    dir_page = f"{hash1}"
                    statement = select(Response).where(Response.hash == hash1)
                    if session.exec(statement).first():
                        continue
                    if os.path.exists(dir_page):
                        sh.rmtree(dir_page)
                    os.mkdir(dir_page)
                    os.chdir(dir_page)

                    req = scrape_page(i, link, LAU, name, hash1)
                    inst.append(req)
                    os.chdir("..")
                    try:
                        os.rmdir(dir_page)
                        continue
                    except OSError:
                        pass

                    for file in os.listdir(dir_page):
                        hash2 = hashf(os.path.join(hash1, file))
                        fname, ext = os.path.splitext(file)
                        inst.append(
                            File_Index(hash=hash1, fname=fname, ext=ext, hash2=hash2)
                        )
            session.add_all(inst)
            session.commit()
            clear_output(wait=True)
        os.chdir("..")

    return


def check_comm(b_href, comm):
    words = re.findall(r"\w+", comm)
    words = [replace_umlaut(word.lower()) for word in words]
    if re.search("|".join(words), b_href):
        return True


def replace_umlaut(string):
    replace = str.maketrans(dict(zip(["ä", "ö", "ü", "ß"], ["ae", "oe", "ue", "ss"])))
    return string.translate(replace)


def is_supplier(b_href, comm):
    if re.sub("(http[s]?://)?(www\.)?", "", b_href) in PATS["WVU"]:
        return Status.CONFIRMED
    elif re.search(PATS["blacklist"], b_href):
        return Status.COMMERCIAL
    elif re.search(PATS["blacklist_press"], b_href):
        return Status.PRESS
    elif re.search(PATS["supplier"], b_href):
        return Status.PROBABLY
    elif check_comm(b_href, comm):
        return Status.COMMUNITY


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


def save_HTML_tabs(tables, j, path=""):
    with pd.ExcelWriter(os.path.join(path, f"{j}_HTML.xlsx")) as writer:
        for i, df in enumerate(tables):
            df.to_excel(writer, sheet_name=f"{i}")
