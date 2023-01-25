import logging
import os
from pathlib import Path

from .config import LOG_FMT

logging.basicConfig(level=logging.INFO, format=LOG_FMT, style="{")
logger = logging.getLogger(__name__)

HOME = Path.cwd()

PATH = Path(os.path.dirname(__file__))
PATH_SUPP = HOME / "supplementary"
PATH_DATA = HOME / "raw_data"
PATH_CONV = HOME / "converted"
PATH_PLOTS = HOME / 'plots'

for P in [PATH_SUPP, PATH_DATA, PATH_CONV, PATH_PLOTS]:
    if not P.exists():
        P.mkdir()
        if P == PATH_SUPP:
            sup_cols = ["name", "url"]
            wvg_cols = ["name", "wvgID", "LAU", "supplied", "discharge_m3_p_d"]
            logger.warn(
                f'Make sure to place:\n1. {"Supplier.xlsx"!r} with columns {sup_cols!r}\n2. {"WVG.xlsx"!r} with columns {wvg_cols!r}\n... in {PATH_SUPP.as_posix()!r}'
            )
