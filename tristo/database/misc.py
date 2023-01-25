import logging
import os
import re
from typing import List

from sqlalchemy import select
from sqlalchemy.orm import Session
from tqdm import tqdm

from ..config import LOG_FMT
from ..paths import PATH_DATA
from ..utils import hashf, make_regex
from .tables import File_Index, Mapping, Param, Regex, Response, Supplier, Unit

logging.basicConfig(level=logging.INFO, format=LOG_FMT, style="{")
logger = logging.getLogger(__name__)


def make_file_index(session: Session):
    stmt = select(Response.hash.distinct())
    to_add = []
    logger.info("Making File Index:")
    for hash1 in tqdm(session.execute(stmt).scalars().all(), desc="[ INFO  ]"):
        path = PATH_DATA / hash1
        if not path.exists():
            continue

        for file in os.listdir(path):
            name, ext = os.path.splitext(file)
            hash2 = hashf(f"{hash1}/{name}.{ext}")
            stmt = select(File_Index.hash2).where(File_Index.hash2 == hash2)

            if session.execute(stmt).first() is not None:
                continue

            to_add.append(File_Index(fname=name, ext=ext, hash=hash1, hash2=hash2))
    session.add_all(to_add)
    session.commit()
    logger.info(f"Added {len(to_add)} files to the index.")


def get_regex(category: str, session: Session) -> List[str]:
    stmt = select(Regex.regex).where(Regex.category == category)
    return session.execute(stmt).scalars().all()


def get_params(session: Session, get_regex=False) -> List[str]:
    stmt = select(Param.param, Param.alias, Param.regex).where(Param.param != None)
    return [
        re.sub("\((?!\?)", "(?:", regex)
        if (regex and get_regex)
        else make_regex(param, alias)
        for param, alias, regex in session.execute(stmt).all()
    ]


def get_units(session: Session) -> Mapping:
    stmt = select(Unit.regex, Unit.unit)
    return {
        re.compile(regex, flags=re.I | re.S): unit
        for regex, unit in session.execute(stmt).all()
    }


def get_supplier_urls(session: Session):
    stmt = select(Supplier.url).where(Supplier.url != None)
    return session.execute(stmt).scalars().all()


def get_keyword_label(session: Session):
    stmt = select(Regex.category.distinct()).where(Regex.id > 0)
    return session.execute(stmt).scalars().all()
