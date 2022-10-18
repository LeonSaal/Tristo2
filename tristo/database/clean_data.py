import logging
import re

import numpy as np
import pandas as pd
from pint import DimensionalityError, UndefinedUnitError, UnitRegistry
from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import Session
from thefuzz import fuzz
from tqdm import tqdm

from ..config import LOG_FMT
from .tables import Data, Param, Regex, Unit

logging.basicConfig(level=logging.INFO, format=LOG_FMT, style="{")
logger = logging.getLogger(__name__)

ureg = UnitRegistry()


def crop_text(text: str, n_chars: int = 8):
    text = text.replace("\n", "")
    if n_chars < 4:
        n_chars = 4
    if len(text) > n_chars:
        text = text[: n_chars - 3] + "..."

    return f"{text:.<{n_chars}}"


def mark_params(session: Session):
    pattern_query = select(Param.id, Param.param, Param.regex).where(
        Param.param != None, Param.param != "None"
    )
    patterns = session.execute(pattern_query).all()

    compiled_patterns = []
    errors = []
    for id, param, regex in patterns:
        if regex == "None":
            continue
        if regex in errors:
            continue

        regex = regex if regex else re.escape(param)

        try:
            reob = re.compile(regex, flags=re.I | re.S | re.M)
        except re.error as e:
            errors.append((id, param, regex, e.msg))
            continue
        compiled_patterns.append((id, param, reob))

    raw_params_query = select(Data.param.distinct()).where(
        Data.omitted_id == None, Data.param_id == None, Data.val != None
    )
    raw_params = session.execute(raw_params_query).scalars().all()

    duplicated = []
    logger.info("Marking parameters:")
    pbar = tqdm(raw_params)
    marked = 0
    for raw_param in pbar:
        pbar.set_postfix_str(crop_text(raw_param, 20))
        cleaned_param = clean_param(raw_param)
        pattern_query = select(Param.id, Param.param, Param.regex).where(
            Param.param != None, Param.param != "None"
        )
        matches = []
        for id, param, reob in compiled_patterns:
            if reob.search(cleaned_param):
                matches.append((id, param, reob.pattern))

        ids = set([m[1] for m in matches])
        if len(ids) == 1:
            id = min([m[0] for m in matches])
            to_update = update(Data).where(Data.param == raw_param).values(param_id=id)
            marked += 1
            session.execute(to_update)
        elif len(ids) == 0:
            continue
        else:
            duplicated.extend([(raw_param, *match) for match in matches])
        session.commit()

    duplicate_unmatched = set(m[0] for m in duplicated)
    unmatched_query = select(Data.param.distinct()).where(
        Data.omitted_id == None,
        Data.param_id == None,
        Data.param.not_in(duplicate_unmatched),
        Data.val != None,
    )
    unmatched = session.execute(unmatched_query).scalars().all()

    fname = "params.xlsx"
    with pd.ExcelWriter("params.xlsx") as writer:
        pd.DataFrame(
            duplicated, columns=["raw_param", "id", "param", "regex"]
        ).to_excel(writer, sheet_name="duplicates")
        pd.DataFrame(errors, columns=["id", "param", "regex", "error_msg"]).to_excel(
            writer, sheet_name="errors"
        )
        pd.DataFrame(unmatched).to_excel(writer, sheet_name="unmatched")

    n_duplicates = len(duplicate_unmatched)
    n_unmatched = len(unmatched)
    n_params = len(raw_params)

    logger.info(
        f"Marked a total of {marked} from {len(raw_params)} entries as parameters ({marked/n_params:.0%})."
    )
    logger.info(
        f"{n_duplicates} entries resulted in multiple matches ({n_duplicates/n_params:.0%})."
    )
    logger.info(f"{n_unmatched} entries remain unmatched ({n_unmatched/n_params:.0%}).")
    logger.info(f"Results are stored in {fname!r}")


def clean_param(param: str):
    trans = str.maketrans(
        {"*": "", "²": "", "³": "", ":": " ", "µ": " ", "Ã": "ä", "¤": "", ".": ","}
    )
    return param.translate(trans)


def mark_blacklist(session: Session):
    pattern_query = select(Regex.id, Regex.regex).where(
        Regex.category == "BLACKLIST_DATA"
    )
    patterns = session.execute(pattern_query).all()
    param_query = select(Data.param.distinct()).where(
        Data.omitted_id == None, Data.param_id == None, Data.val != None
    )
    params = session.execute(param_query).scalars().all()

    compiled_patterns = [
        (id, re.compile(regex, flags=re.I | re.S))
        for id, regex in patterns
        if regex != "None"
    ]

    logger.info("Marking blacklist:")
    marked = 0
    for param in (pbar := tqdm(params, mininterval=0.5)):
        pbar.set_postfix_str(crop_text(param, 20))

        for id, reob in compiled_patterns:
            if not reob.search(param):
                continue
            omitted_id = update(Data).where(Data.param == param).values(omitted_id=id)
            session.execute(omitted_id)
            marked += 1
            break
        session.commit()
    logger.info(
        f"Marked a total of {marked} from {len(params)} entries as blacklist ({marked/len(params):.0%})."
    )


def mark_data(session: Session, overwrite=False):
    if overwrite:
        set_none = update(Data).values(omitted_id=None, param_id=None)
        session.execute(set_none)
    mark_blacklist(session=session)
    mark_params(session=session)


def get_vals(param, session: Session, unit: str = None, limit_thresh=1.2):
    if isinstance(param, int):
        vals = (
            select(Data.val, Data.unit, Param.param, Param.unit, Param.limit)
            .join(Param)
            .where(Data.param_id == param, Data.val != None, Data.unit != None)
        )
    elif isinstance(param, str):
        vals = (
            select(Data.val, Data.unit, Param.param, Param.unit, Param.limit)
            .join(Param)
            .where(Param.param.regexp_match(param), Data.val != None, Data.unit != None)
        )

    out = []
    for val, raw_unit, param, to_unit, limit in session.execute(vals).all():
        if limit:
            limit = clean_val(limit)
        units = select(Unit.unit, Unit.regex)
        from_unit = None
        for unit, regex in session.execute(units).all():
            if re.match(regex, raw_unit, flags=re.I):
                from_unit = unit
                break
        if (
            (from_unit is not None)
            and (to_unit is not None)
            and (val := clean_val(val))
        ):
            try:
                converted_val = (
                    ureg.Quantity(f"{val} {from_unit}").to(to_unit).magnitude
                )

                if converted_val == limit:
                    continue
                out.append(converted_val)
            except DimensionalityError:
                logger.error(f"unable to convert from {from_unit} to {to_unit}")
            except UndefinedUnitError:
                logger.error(from_unit, to_unit, raw_unit)

    out = np.array(out)
    bg_mask = out < 0
    bg = -out[bg_mask]
    if limit:
        outlier_mask = out > limit_thresh * limit
    else:
        outlier_mask = False
    outliers = out[outlier_mask]
    vals = out[~bg_mask & ~outlier_mask]
    return vals, bg, outliers, param, to_unit, limit


def clean_val(val: str):
    val = val.replace(" ", "")
    num_pat = "[<-]?\d+([\.,]\d+)?"
    if not re.fullmatch(num_pat, val):
        return
    else:
        return float(val.replace(",", ".").replace("<", "-"))


def rem_dup_param(session: Session):
    stmt = (
        select(Param.param, Param.regex, func.count(Param.param))
        .where(Param.origin == None)
        .group_by(Param.param, Param.regex)
        .having(func.count(Param.param) > 1)
    )
    for param, regex, _ in session.execute(stmt):
        stmt = select(func.min(Param.id)).where(
            Param.param == param, Param.regex == regex
        )
        for id in session.execute(stmt).scalars():
            to_del = delete(Param).where(Param.id != id)
            session.execute(to_del)


def mark_limit_col(session: Session, thresh=0.75):
    stmt = (
        select(Data.hash2, Data.tab, Data.col)
        .where(Data.omitted_id == None)
        .group_by(Data.hash2, Data.tab, Data.col)
    )
    logger.info("Marking Limit Columns")
    table_query = select(Data.hash2, Data.tab).group_by(Data.hash2, Data.tab)
    n_tabs = len(session.execute(table_query).all())
    n_limit_cols = 0
    for hash2, tab, col in (pbar := tqdm(session.execute(stmt).all(), mininterval=0.1)):
        pbar.set_postfix_str(f"{hash2}: table {tab}, column {col}")
        col_data = (
            select(Data.val, Param.limit)
            .where(
                Data.hash2 == hash2,
                Data.tab == tab,
                Data.col == col,
                Data.param_id != None,
                Data.val != None,
                Data.val != "-",
            )
            .join(Param)
        )
        vals = session.execute(col_data).all()
        score_sum = sum(
            fuzz.partial_ratio(
                val.replace(",", "."), limit.replace(",", ".") if limit else limit
            )
            for val, limit in vals
        )
        rel_score = score_sum / len(vals) / 100
        if rel_score > thresh:
            to_update = (
                update(Data)
                .where(Data.hash2 == hash2, Data.tab == tab, Data.col == col)
                .values(omitted_id=-1)
            )
            session.execute(to_update)
            n_limit_cols += 1
    session.commit()
    logger.info(
        f"Marked a total of {n_limit_cols} limit columns in {n_tabs} tables ({n_limit_cols/n_tabs:.0%}). "
    )
