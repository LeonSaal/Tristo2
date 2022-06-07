# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 10:18:42 2022

@author: Leon
"""

import calendar
from hashlib import sha1
import locale
import os
import pandas as pd
import re
from selenium.webdriver.chrome.service import Service


def hashf(x):
    return sha1(x.encode("utf-8")).hexdigest()


PATH = os.path.realpath(os.path.dirname(__file__))
PATH_SUPP = os.path.join(PATH, "supplementary")

SERVICE = Service(os.path.join(PATH_SUPP, "chromedriver.exe"))
TIMEOUT = 5

HOME = os.getcwd()

os.chdir(HOME)

regex_file = os.path.join(PATH_SUPP, "REGEX_PARAM.xlsx")
REGEX_PARAM = pd.read_excel(
    regex_file, header=0, dtype={"value": "string"}, usecols=[0, 1], sheet_name=None
)
TO_REPLACE = {}
for sheet, df in REGEX_PARAM.items():
    df.fillna("", inplace=True)
    df.dropna(axis=0, inplace=True)
    TO_REPLACE[sheet] = {
        pat: value for pat, value in zip(df["to_replace"].values, df["value"].values)
    }  # REGEX_PARAM['to_replace'].to_list()

locale.setlocale(locale.LC_ALL, "deu_deu")
MONTHS = {name: str(i) for i, name in enumerate(calendar.month_name)}

keywords_file = os.path.join(PATH_SUPP, "KEYWORDS.xlsx")
KEYWORDS = pd.read_excel(keywords_file, header=0)
KEYWORDS = {key: KEYWORDS[key].dropna().to_list() for key in KEYWORDS.columns}

limits_file = os.path.join(PATH_SUPP, "LIMITS.xlsx")
LIMITS = pd.read_excel(limits_file, header=0, index_col=0, sheet_name=None)

WVU_file = os.path.join(PATH_SUPP, "Wasser VU mit Webseite.xlsx")
WVU = pd.read_excel(WVU_file, usecols=[2]).dropna().squeeze().to_list()

THRESH = {}
params = []
UNITS = {}
MOLAR_MASS = {}
for sheet_name, df in LIMITS.items():
    params.extend(df.index.dropna().values)
    if "Wert" in df:
        threshs = df["Wert"].dropna()
        pat = r"\s[(].*[)]"
        THRESH.update(
            {
                param: value
                for param, value in zip(
                    threshs.index.str.replace(pat, "", regex=True), threshs.values
                )
                if param not in THRESH
            }
        )
    if "to_replace" in df:
        to_replace = df.copy().dropna(subset=["to_replace"])
        TO_REPLACE["index"].update(
            {
                pat: param
                for pat, param in zip(
                    to_replace["to_replace"].values, to_replace.index.values
                )
            }
        )
    if "Einheit" in df:
        units = df.copy().dropna(subset=["Einheit"])
        UNITS.update(
            {
                param: unit
                for param, unit in zip(units.index.values, units["Einheit"].values)
                if param not in UNITS
            }
        )
    if "M" in df:
        molar_mass = df.copy().dropna(subset=["M"])
        MOLAR_MASS.update(
            {
                param: M
                for param, M in zip(molar_mass.index.values, molar_mass["M"].values)
            }
        )
    else:
        continue
    if "Alias" in df:
        alias = df[df["Alias"].notna()].set_index("Alias")
        params.extend(alias.index.values)
        THRESH.update(
            {param: value for param, value in zip(alias.index, alias["Wert"])}
        )
KEYWORDS["params"] = [f"{re.escape(param)}" for param in params]
KEYWORDS["WVU"] = WVU

PATS = {key: "|".join(keywords) for key, keywords in KEYWORDS.items()}

