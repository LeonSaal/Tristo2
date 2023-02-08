# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 10:10:09 2022

@author: Leon
"""

from sqlalchemy import select

from .database import GN250, GV3Q, OpenDB


def get_districts_from_comm(comm: str):
    with OpenDB().session() as session:
        stmt = select(GN250.name).join(GV3Q).where(GV3Q.name == comm)
        districts = set([district for district in session.execute(stmt).scalars()])
    if districts == {}:
        return {comm}
    else:
        return set(districts)

