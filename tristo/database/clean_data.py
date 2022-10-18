import re

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pint import DimensionalityError, UndefinedUnitError, UnitRegistry
from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import Session
from tqdm import tqdm

from .tables import Data, Param, Regex, Unit

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
    print("Marking parameters:")
    pbar = tqdm(raw_params)

    for raw_param in pbar:
        pbar.set_description(crop_text(raw_param, 20))
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
            session.execute(to_update)
        elif len(ids) == 0:
            continue
        else:
            duplicated.extend([(raw_param, *match) for match in matches])

    duplicate_unmatched = [m[0] for m in duplicated]
    unmatched_query = select(Data.param.distinct()).where(
        Data.omitted_id == None,
        Data.param_id == None,
        Data.param.not_in(duplicate_unmatched),
        Data.val != None,
    )
    unmatched = session.execute(unmatched_query).scalars().all()

    with pd.ExcelWriter("params.xlsx") as writer:
        pd.DataFrame(
            duplicated, columns=["raw_param", "id", "param", "regex"]
        ).to_excel(writer, sheet_name="duplicates")
        pd.DataFrame(errors, columns=["id", "param", "regex", "error_msg"]).to_excel(
            writer, sheet_name="errors"
        )
        pd.DataFrame(unmatched).to_excel(writer, sheet_name="unmatched")


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

    print("Marking blacklist:")

    for param in (pbar := tqdm(params, mininterval=0.5)):
        pbar.set_description(crop_text(param, 20))

        for id, reob in compiled_patterns:
            if not reob.search(param):
                continue
            omitted_id = update(Data).where(Data.param == param).values(omitted_id=id)
            session.execute(omitted_id)
            break


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
                print(f"unable to convert from {from_unit} to {to_unit}")
            except UndefinedUnitError:
                print(from_unit, to_unit, raw_unit)

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


def boxplot_param(vals, bg, outliers, param_name, unit, limit):
    plt.boxplot(vals)
    plt.boxplot(bg, positions=[2])
    if limit:
        plt.ylim(0, 1.2 * limit)
    else:
        if unit == "µg/l":
            plt.ylim(0, 5)
    xlims = plt.xlim()
    plt.hlines([limit], *xlims, colors=["red"], lw=1)
    plt.xticks([1, 2], labels=[f"> BG,  N = {len(vals)}", f"< BG: N={len(bg)}"])
    plt.ylabel(f"{unit}")
    plt.title(param_name)
    plt.show()
    total_len = len(vals) + len(bg) + len(outliers)
    if total_len == 0:
        total_len = 1
    print(
        "; ".join(
            [
                f"{name}: N={len(x)} ({len(x)/total_len:.2%})"
                for name, x in zip(
                    ["> BG", "< BG", "not validated"], [vals, bg, outliers]
                )
            ]
        )
    )


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
