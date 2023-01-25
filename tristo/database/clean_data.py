import datetime as dt
import logging
import re

import pandas as pd
from pint import UnitRegistry
from sqlalchemy import Float, and_, cast, delete, func, select, update
from sqlalchemy.orm import Session
from tqdm import tqdm

from ..config import LOG_FMT
from ..utils import crop_text, make_regex
from .tables import Data, Param, Regex, Unit

ureg = UnitRegistry()

# defining special units
ureg.define("NTU=1")
ureg.define("MPN=1")
ureg.define("FNU=NTU")
ureg.define("percent=0.01")
ureg.define("KBE=1")
ureg.define("Anzahl=1")
ureg.define("pH-Einheiten=1")
ureg.define("TON=1")
ureg.define("mval=0.5*mmol")
ureg.define("degreedH=0.1783*mmol/l")
ureg.define("degreefH=0.56*degreedH")
ureg.define("O2=1")
ureg.define("c_eq=mol/l")

logging.basicConfig(level=logging.INFO, format=LOG_FMT, style="{")
logger = logging.getLogger(__name__)


def mark_params(session: Session):
    pattern_query = select(Param.id, Param.param, Param.alias, Param.regex).where(
        Param.param != None, Param.param != "None"
    )
    patterns = session.execute(pattern_query).all()

    compiled_patterns = []
    errors = []
    for id, param, alias, regex in patterns:
        if regex == "None":
            continue
        if regex in errors:
            continue

        regex = regex if regex else make_regex(param, alias)

        try:
            reob = re.compile(regex, flags=re.I | re.S | re.M)
        except re.error as e:
            errors.append((id, param, regex, e.msg))
            continue
        compiled_patterns.append((id, param, reob))

    raw_params_query = (
        select(Data.param, func.count(Data.id).label("count"))
        .where(Data.omitted_id == None, Data.param_id == None, Data.val != None)
        .group_by(Data.param)
        .order_by(func.count(Data.id).desc())
    )
    raw_params = session.execute(raw_params_query).all()

    duplicated = []
    logger.info("Marking parameters:")
    pbar = tqdm(raw_params, desc="[ INFO  ]")
    marked = 0
    for raw_param, count in pbar:
        pbar.set_postfix_str(crop_text(raw_param, 30))
        cleaned_param = clean_param(raw_param)
        pattern_query = select(Param.id, Param.param, Param.regex).where(
            Param.param != None, Param.param != "None"
        )
        matches = []
        for id, param, reob in compiled_patterns:
            if reob.search(cleaned_param):
                matches.append((id, param, reob.pattern))

        params = set(m[1] for m in matches)

        if len(params) == 1:
            id_query = select(func.min(Param.id)).where(
                Param.param == params.pop()
            )  # min([m[0] for m in matches])
            id = session.execute(id_query).scalar()
            to_update = update(Data).where(Data.param == raw_param).values(param_id=id)
            session.execute(to_update)
            marked += 1

        elif len(params) == 0:
            continue

        else:
            ordered_matches = sorted(matches, key=lambda x: len(x[1]))

            # check if match with shortest length is in all other matches --> likely parent and metabolite
            cond_1 = all(
                [
                    ordered_matches[0][1].lower() in match[1].lower()
                    for match in ordered_matches[1:]
                ]
            )
            # check if all other matches are in longest match --> likely composite
            cond_2 = all(
                [
                    match[1].lower() in ordered_matches[-1][1].lower()
                    for match in ordered_matches[:-1]
                ]
            )
            if not (cond_1 or cond_2):
                duplicated.extend([(raw_param, count, *match) for match in matches])
                continue

            id_query = select(func.min(Param.id)).where(
                Param.param == ordered_matches[-1][1]
            )
            id = session.execute(id_query).scalar()  # ordered_matches[-1][0]
            to_update = update(Data).where(Data.param == raw_param).values(param_id=id)
            session.execute(to_update)
            marked += 1

        session.commit()

    duplicate_unmatched = set(m[0] for m in duplicated)
    unmatched_query = (
        select(Data.param, func.count(Data.id))
        .where(
            Data.omitted_id == None,
            Data.param_id == None,
            Data.param.not_in(duplicate_unmatched),
            Data.val != None,
        )
        .group_by(Data.param)
        .order_by(func.count(Data.id).desc())
    )
    unmatched = session.execute(unmatched_query).all()

    now = dt.datetime.now().strftime("%y%m%d-%H_%M")
    fname = f"params_{now}.xlsx"
    with pd.ExcelWriter(fname) as writer:
        pd.DataFrame(
            duplicated, columns=["raw_param", "count", "id", "param", "regex"]
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
        Data.param_id == None, Data.omitted_id == None, Data.val != None
    )
    params = session.execute(param_query).scalars().all()

    compiled_patterns = [
        (id, re.compile(regex, flags=re.I | re.S))
        for id, regex in patterns
        if regex != "None"
    ]

    logger.info("Marking blacklist:")
    marked = 0
    for param in (pbar := tqdm(params, mininterval=0.5, desc="[ INFO  ]")):
        pbar.set_postfix_str(crop_text(param, 30))

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
        set_none = update(Data).values(param_id=None, omitted_id=None)
        del_regex = delete(Regex).where(Regex.regex == "None")
        session.execute(set_none)
        session.execute(del_regex)
        session.commit()
    mark_blacklist(session=session)
    mark_params(session=session)


def clean_data_table(session: Session, overwrite=False):
    if overwrite:
        set_none = update(Data).values(val_num=None, category=None, unit_factor=None)
        session.execute(set_none)
        session.commit()
    clean_vals(session=session)
    get_unit_factor(session=session)
    mark_limit_cols(session=session)
    mark_number_cols(session=session)


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


def get_unit_factor(session: Session):
    logger.info("Setting unit factor:")
    unit_regex = {}
    unit_kinds = select(Unit.kind).distinct()
    for kind in session.execute(unit_kinds).scalars().all():
        sub_dict = {}
        units = select(Unit.unit, Unit.regex).where(Unit.kind == kind)
        for unit, regex in session.execute(units):
            sub_dict[regex] = unit

        unit_regex[kind] = sub_dict

    params = (
        select(Param.param, Data.param_id, Param.unit, Unit.kind)
        .distinct()
        .join(Param)
        .join(Unit, Unit.unit == Param.unit)
    )
    for param, id, to_unit, kind in (
        pbar := tqdm(session.execute(params).all(), desc="[ INFO  ]")
    ):
        for regex, unit in unit_regex[kind].items():

            if unit == to_unit:
                factor = 1
            else:
                factor = ureg.Quantity(unit).to(to_unit).magnitude

            pbar.set_postfix_str(f"{param}: {unit} -> {factor:.3E} x {to_unit}")
            upd = (
                update(Data)
                .where(
                    Data.param_id == id,
                    Data.val_num != None,
                    func.lower(Data.unit).regexp_match(f".*{regex}"),
                )
                .values(unit_factor=factor)
                .execution_options(synchronize_session="fetch")
            )
            session.execute(upd)
            session.commit()


def mark_limit_cols(session: Session):
    logger.info("Marking limit columns:")
    cols_query = (
        select(Data.hash2, Data.tab, Data.col)
        .distinct()
        .join(Param)
        .where(
            and_(
                Param.limit != None,
                cast(Param.limit, Float) != 0,
                Data.val_num != None,
                Data.unit_factor != None,
                (
                    func.abs(Data.val_num * Data.unit_factor - cast(Param.limit, Float))
                    <= 1e-6
                )
            )
            | and_(Param.id == 48, Data.val.like("6_5%-%9_5"))
            | (Data.val.like("%grenz%"))
        )
    )

    for hash2, tab, col in (
        pbar := tqdm(session.execute(cols_query).all(), desc="[ INFO  ]")
    ):
        pbar.set_postfix_str(f"{hash2}: table {tab}, column {col}")
        upd = (
            update(Data)
            .where(Data.col == col, Data.hash2 == hash2, Data.tab == tab)
            .values(omitted_id=-1)
        )
        session.execute(upd)
    session.commit()


def mark_number_cols(session: Session):
    logger.info("Marking number columns:")
    cols_query = (
        select(Data.hash2, Data.tab)
        .where(Data.col == 0, Data.param_id != None, Data.val_num != None)
        .group_by(Data.hash2, Data.tab)
    )
    N = 0
    for hash2, tab in (
        pbar := tqdm(session.execute(cols_query).all(), desc="[ INFO  ]")
    ):
        pbar.set_postfix_str(f"{hash2}: table {tab}")

        col = select(Data.val_num).where(
            Data.hash2 == hash2, Data.tab == tab, Data.col == 0
        )
        vals = pd.read_sql(col, session.connection())
        if 1 not in vals.diff().mode().T.values:
            continue

        N += 1
        upd = (
            update(Data)
            .where((Data.hash2 == hash2) & (Data.tab == tab) & (Data.col == 0))
            .values(omitted_id=-2)
        )
        session.execute(upd)
    session.commit()
    print(f"Marked {N} columns as pagination.")


def clean_val_db(column):
    logger.info("Cleaning values:")
    temp = func.trim(column)

    replacements = {
        "\n": "",
        "mg/L": "",
        "<": "",
        "˂": "",
        "<": "",
        "{": "",
        "}": "",
        "|": "",
        "[": "",
        "]": "",
        "(": "",
        ")": "",
        ":": "",
        ", ": ".",
        "*": "",
        "-": "",
        "–": "",
        "0 ,": "0.",
        "0 .": "0.",
        "0 ": "0.",
        " ": "",
        " ": "",
        ",": ".",
    }
    for pat, sub in replacements.items():
        temp = func.replace(temp, pat, sub)

    return cast(temp, Float)


def clean_vals(session: Session):
    dash = "-–"
    lt = "<˂"
    pats = {
        "range": f"^[{dash}{lt}]?\s*\d+[,\.\d]*\s*[{dash}]+\s*\d+[,\.\d]*\s*$",
        "all_text": "^\D+$",
        "LOQ": f"^\W*[{dash}{lt}]\s*\d|(^|.*[\s\W])u[{dash}\.\s]*b",
        "LOD": f"(^|.*[{dash}\s\W])[nu][\.\s]*n|^0([,\.]0*)?$",
        "not_measured": f"(^|.*[\s\W])n[{dash}\.\s]*[gb]|.*o(\.|hne)\s*[gb](efund)?",
        "blank": f"^[{dash}_\s]+$",
    }
    # update vals
    upd_val = (
        update(Data)
        .values(val_num=clean_val_db(Data.val))
        .where(
            Data.val.regexp_match(pats["range"]) != True,
            Data.val.regexp_match(pats["all_text"]) != True,
            Data.param_id != None,
            func.instr(Data.val, func.char(10)) == False,
        )
        .execution_options(synchronize_session="fetch")
    )
    session.execute(upd_val)
    session.commit()

    # Update category
    upd_bg = (
        update(Data)
        .where(
            func.lower(Data.val).regexp_match(pats["LOQ"], flags=re.I),
            Data.param_id.not_in([49, 867, 771]),
            Data.param_id != None,
            Data.omitted_id == None,
        )
        .values(category="BG")
        .execution_options(synchronize_session="fetch")
    )

    upd_nn = (
        update(Data)
        .where(
            (
                (func.lower(Data.val).regexp_match(pats["LOD"], flags=re.I))
                | (Data.val_num == 0)
            )
            & (Data.param_id != None)
            & (Data.omitted_id == None)
            & (Data.category == None)
            & (
                Data.param_id.not_in(
                    [
                        1,
                        2,
                        33,
                        34,
                        39,
                        40,
                        568,
                        629,
                        634,
                        749,
                        1139,
                        1142,
                        1265,
                        1462,
                        1473,
                    ]
                )
            ),
        )
        .values(category="NN")
        .execution_options(synchronize_session="fetch")
    )

    upd_nb = (
        update(Data)
        .where(
            (
                func.lower(Data.val).regexp_match(pats["not_measured"])
                | Data.val.regexp_match(pats["blank"])
            )
            & (Data.param_id != None)
            & (Data.category == None)
            & (Data.omitted_id == None)
        )
        .values(category="NB")
        .execution_options(synchronize_session="fetch")
    )

    upd_range = (
        update(Data)
        .where(
            Data.val.regexp_match(pats["range"]) == True,
            Data.val.not_like('"6_5%-%9_5"'),
            Data.param_id != None,
            Data.category == None,
            Data.omitted_id == None,
        )
        .values(category="RANGE")
        .execution_options(synchronize_session="fetch")
    )

    upd_val = (
        update(Data)
        .where(
            (Data.param_id != None)
            & (Data.category == None)
            & (Data.omitted_id == None)
        )
        .values(category="> BG")
    )

    for upd in [upd_bg, upd_nb, upd_nn, upd_range, upd_val]:
        session.execute(upd)
        session.commit()


# def mark_limit_col(session: Session, thresh=0.75, overwrite=False):
#     if overwrite:
#         set_none = update(TableData).values(legal_lim=None)
#         session.execute(set_none)

#     stmt = (
#         select(Data.hash2, Data.tab, Data.col).join(TableData, TableData.hash2==Data.hash2)
#         .where(Data.omitted_id == None, TableData.legal_lim==None)
#         .group_by(Data.hash2, Data.tab, Data.col)
#     )
#     logger.info("Marking Limit Columns:")
#     table_query = select(Data.hash2, Data.tab).group_by(Data.hash2, Data.tab)
#     n_tabs = len(session.execute(table_query).all())
#     n_limit_cols = 0
#     for hash2, tab, col in (
#         pbar := tqdm(session.execute(stmt).all(), mininterval=0.1, desc="[ INFO  ]")
#     ):
#         pbar.set_postfix_str(f"{hash2}: table {tab}, column {col}")
#         col_data = (
#             select(Data.val, Param.limit)
#             .where(
#                 Data.hash2 == hash2,
#                 Data.tab == tab,
#                 Data.col == col,
#                 Data.param_id != None,
#                 Data.val != None,
#                 Data.val != "-",
#             )
#             .join(Param)
#         )
#         vals = session.execute(col_data).all()
#         if not vals:
#             continue
#         score_sum = sum(
#             fuzz.partial_ratio(
#                 val.replace(",", "."), limit.replace(",", ".") if limit else limit
#             )
#             for val, limit in vals
#         )
#         rel_score = score_sum / len(vals) / 100
#         if rel_score > thresh:
#             to_update = (
#                 update(Data)
#                 .where(Data.hash2 == hash2, Data.tab == tab, Data.col == col)
#                 .values(omitted_id=-1)
#             )
#             session.execute(to_update)
#             to_update = (update(TableData).where(TableData.hash2==hash2, TableData.tab==tab).values(legal_lim=True))
#             session.execute(to_update)
#             n_limit_cols += 1
#     session.commit()
#     logger.info(
#         f"Marked a total of {n_limit_cols} limit columns in {n_tabs} tables ({n_limit_cols/n_tabs:.0%}). "
#     )


# def mark_limit_col(session: Session, thresh=0.1, overwrite=False):
#     if overwrite:
#         set_none = update(TableData).values(legal_lim=None)
#         session.execute(set_none)

#     stmt = (
#         select(Data.hash2, Data.tab, Data.col)
#         .join(TableData, TableData.hash2 == Data.hash2)
#         .where(Data.omitted_id == None, TableData.legal_lim == None)
#         .group_by(Data.hash2, Data.tab, Data.col)
#     )
#     logger.info("Marking Limit Columns:")
#     table_query = select(Data.hash2, Data.tab).group_by(Data.hash2, Data.tab)
#     n_tabs = len(session.execute(table_query).all())
#     n_limit_cols = 0
#     for hash2, tab, col in (
#         pbar := tqdm(session.execute(stmt).all(), mininterval=0.1, desc="[ INFO  ]")
#     ):
#         pbar.set_postfix_str(f"{hash2}: table {tab}, column {col}")
#         col_data = (
#             select(Data.val, Data.unit, Param.limit, Param.unit)
#             .where(
#                 Data.hash2 == hash2,
#                 Data.tab == tab,
#                 Data.col == col,
#                 Data.param_id != None,
#                 Data.val != None,
#             )
#             .join(Param)
#         )
#         val_df = pd.read_sql(col_data, session.connection(), coerce_float=False)
#         val_df = df_clean_unit(val_df, session, cols=["unit", "unit_1"])

#         val_df.val = val_df.val.apply(clean_val)
#         val_df = val_df.dropna()

#         if val_df.empty:
#             continue

#         val_df.val = val_df.T.apply(lambda x: convert_val(x.val, x.unit, x.unit_1)).T
#         val_df = val_df.apply(lambda x: pd.to_numeric(x, errors="coerce"))
#         vals = val_df.val
#         limits = val_df.limit
#         isclose = np.mean(np.isclose(vals, limits))

#         if isclose > thresh:
#             to_update = (
#                 update(Data)
#                 .where(Data.hash2 == hash2, Data.tab == tab, Data.col == col)
#                 .values(omitted_id=-1)
#             )
#             session.execute(to_update)
#             to_update = (
#                 update(TableData)
#                 .where(TableData.hash2 == hash2, TableData.tab == tab)
#                 .values(legal_lim=True)
#             )
#             session.execute(to_update)
#             n_limit_cols += 1
#     session.commit()
#     logger.info(
#         f"Marked a total of {n_limit_cols} limit columns in {n_tabs} tables ({n_limit_cols/n_tabs:.0%}). "
#     )

# def clean_val(val: str):
#     val = val.replace(" ", "")
#     num_pat = "[<-]?\d+([\.,]\d+)?"
#     if not re.fullmatch(num_pat, val):
#         return None
#     else:
#         return abs(float(val.replace(",", ".").replace("<", "")))  #


# def df_clean_unit(df: pd.DataFrame, session: Session, cols="unit"):
#     unit_query = select(Unit.regex, Unit.unit)
#     units_re = {f".*{regex}.*": unit for regex, unit in session.execute(unit_query)}
#     spec_ops = {op: "" for op in ["[\]\[]", "\n", "%", "\*"]}
#     df.loc[:, cols] = df.replace(regex=spec_ops).loc[:, cols]
#     df.loc[:, cols] = df.replace(regex=units_re).loc[:, cols]
#     return df


# def df_clean_val(df: pd.DataFrame):
#     pass
