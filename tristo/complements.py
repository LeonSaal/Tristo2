# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 10:18:42 2022

@author: Leon
"""
import calendar
import locale
import logging

from .config import LOG_FMT
from .paths import PATH_SUPP

logging.basicConfig(level=logging.INFO, format=LOG_FMT, style="{")
logger = logging.getLogger(__name__)
locale.setlocale(locale.LC_ALL, "deu_deu")

from selenium.webdriver.chrome.service import Service

from .database import (OpenDB, get_keyword_label, get_params, get_regex,
                       get_supplier_urls, get_units)
from .paths import PATH_SUPP

SERVICE_FILE = "chromedriver.exe"
PATH_SERVICE = PATH_SUPP / SERVICE_FILE
if not PATH_SERVICE.exists():
    DRIVER_URL = "https://chromedriver.chromium.org/downloads"
    logger.warn(
        f"Please supply {SERVICE_FILE!r} in {PATH_SUPP.as_posix()!r} before scraping!\n\t-> Current versions can be found at {DRIVER_URL!r}."
    )
SERVICE = Service(PATH_SERVICE)
TIMEOUT = 5
MONTHS = {name: str(i) for i, name in enumerate(calendar.month_name)}

with OpenDB().session() as session:
    UNITS = get_units(session=session)
    KEYWORD_LABEL = get_keyword_label(session=session)
    KEYWORDS = {key: get_regex(key, session=session) for key in KEYWORD_LABEL}
    KEYWORDS["PARAMS"] = get_params(session=session, get_regex=True)
    KEYWORDS["RAW_PARAMS"] = get_params(session=session)
    KEYWORDS["SUPPLIER_LIST"] = get_supplier_urls(session=session)
    KEYWORDS['UNIT']= [regex.pattern for regex in UNITS.keys()]
PATS = {key: "|".join(keywords) for key, keywords in KEYWORDS.items()}


