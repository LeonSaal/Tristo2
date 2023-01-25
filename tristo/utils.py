import logging
import re
import sys
from hashlib import sha1
from typing import List

import pandas as pd
import wget

from .config import LOG_FMT
from .paths import PATH_SUPP

logging.basicConfig(level=logging.INFO, format=LOG_FMT, style="{")
logger = logging.getLogger(__name__)


def count_occ(dfs: List[pd.DataFrame], pat: str) -> int:
    return int(
        sum(
            [
                df.reset_index()
                .apply(lambda x: x.astype(str).str.contains(pat))
                .to_numpy()
                .sum()
                for df in dfs
            ]
        )
    )


def hashf(x: str):
    return sha1(x.encode("utf-8")).hexdigest()


def is_number(value: str or float) -> bool:
    value = str(value)
    pat = r"\A([~\s+-<]*\d+([\.,]\d+)?[~\s+-±]*)+\Z"
    if re.match(pat, value, flags = re.M) or value == "nan":
        return True
    else:
        return False


def crop_text(text: str, n_chars: int = 8):
    text = text.replace("\n", "")
    if n_chars < 4:
        n_chars = 4
    if len(text) > n_chars:
        text = text[: n_chars - 3] + "..."

    return f"{text: <{n_chars}}"



def make_regex(param: str, alias: str):
    sep = '###'
    len_thresh = 7
    replacements = {
        # make last char optional 
        #f"(?<=.{{{len_thresh}}})(?P<last>[^(\d])$":'\g<last>?',

        # escape special characters
        f"\s[(](?P<acro>[-\w\s()]+)[)](?=\Z|{sep})": f"{sep}\g<acro>",
        "[[]": "\[",
        "[]]": "\]",
        "[,\.]": r"[,.\\s]*",
        "[(]": "\(?",
        "[)]": "\)?",
        "[+]": "\+",
        "´": "´?",

        # common synonyms/ misspells /ocr difficulties
        "chlor": "ch?loro?",
        "brom": "bromo?",
        "iod": "iodo?",
        "fluor": "fl?uoro?",

        # make first char optional if its not numeric
        f"^(?P<first>[a-z])(?=[^\s-]{{{len_thresh},}})":'\g<first>?',

        # common synonyms/ misspells /ocr difficulties
        "nsäure": "n(säure|o?at)",
        "eco": "er?co",
        'enzo':'enzo?',
        "alpha": r"(alpha|α|\\ba)",
        "beta": "(beta|ß|b)",
        "gamma": r"(amma|γ|c|Γ|\\br)",
        "delta": "(delta|δ|d)",
        "zeta": "(zeta|ζ|z).",
        "epsilon": "(epsilon|ε|e)",
        'ili':'ill?i',
        "(?P<l>[zul])(en|o\??l)": "\g<l>(ene|ole)",
        "then": "th(yl)?en",
        "ala": "ala?",
        "ch?(?=[^\?])": "ch?",
        "t((?=[yaeiuo])|h)": "th?",
        "(?<=[nld])e(?!^\?)": "e?",
        "in": "(i|e)e?n",
        "ut": "uth?",
        "id(?!$)": "i[dt]",
        "ck": "c?k",
        "ur": "uo?r",
        'ff':'(ff|ﬀ)',
        "(?<=[^-\s])(ph(?=\S)|f)": "(ph?|f)",
        "mbd": "mb?d",
        "ä": "([aäd4]|ae)?",
        "ö": "([oödi]|oe)?",
        "ü": "([uü]|ue)?",
        '[cz]':'(c|z)',
        "[1lijy]": "[1lijyv\\|!]",
        "[ck]": "[ck]",
        "uo": "[ou]{1,2}",
        "[\s-]": r"[-—\\s]*",
        "g": "[gq]",
        'm':'(m|rn)',
        "\((?!\?)": "(?:",
    }
    
    if alias:
        param = sep.join(filter(lambda x:x, [param, alias]))
 
    for pat, replacement in replacements.items():
        param = re.sub(pat, replacement, param, flags=re.I)
    prelim = r'\b'
    postlim = r''
    return '|'.join([f'{prelim}{sub_pat}{postlim}' for sub_pat in param.split(sep)])



def bar_progress(
    current, total, width=80
):  # https://stackoverflow.com/questions/58125279/python-wget-module-doesnt-show-progress-bar
    progress_message = (
        f"Downloading: {current/total:3.0%} [{current/1e6:5.1f} / {total/1e6:5.1f}] MB"
    )
    # Don't use print() as it will print in new line every time.
    sys.stdout.write("\r" + progress_message)
    sys.stdout.flush()

def download_file(url, msg, out = PATH_SUPP.as_posix()):
    logger.info(f"Loading {msg} from {url!r}")
    return wget.download(url, bar=bar_progress, out=out)
