# -*- coding: utf-8 -*-
"""
Created on Thu Jan 20 15:34:56 2022

@author: Leon
"""
import os
import re

import numpy as np
import pandas as pd

from .complements import PATS


def get_col(df, pat: str, thresh=0.3):
    mask = df.apply(lambda x: x.astype(str).str.contains(pat, flags=re.I)).sum()
    if type(thresh) == float:
        rel_mask = (mask**2 /(df.count() * mask.max()))
        return rel_mask[rel_mask >= thresh].index
    elif type(thresh) == int:
        return mask[mask >= thresh].index


def path_from_index(idx, ext):
    return os.path.join(os.getcwd(), f"{idx[0]}", f"{idx[1]}{ext}")


def get_index_from_path(path):
    path_fwd = path.replace(os.sep, os.altsep)
    find = re.search(r"/(?P<hash>[a-f0-9]{40})", path_fwd)
    if find:
        return find.group("hash")
    else:
        return None


def make_file_index(fnames, hash_folder):
    df = pd.DataFrame(index=fnames)
    df.index.rename("fname", inplace=True)
    out = pd.concat([df], keys=[(hash_folder)], names=["hash"])
    return out


def make_index_col(df, **kwargs):
    col = get_col(df, PATS["PARAMS"], **kwargs)
    if len(col) == 1:
        params = pd.Series(df[col].squeeze())
        df.drop(col, inplace=True, axis=1)
        irange = pd.Series(np.arange(df.index.size))
        df.insert(
            0, "param", params.fillna(method="ffill").fillna(value=irange).to_list()
        )  # .replace(
        # regex=TO_REPLACE['index']).fillna(method='ffill').fillna(value=irange))
    return df


def get_orientation(df, pat, thresh=0.5):
    mask = df.apply(lambda x: x.astype("string").str.match(pat))
    rel_mask_0 = mask.sum(axis=0).divide(df.shape[0])
    rel_mask_1 = mask.sum(axis=1).divide(df.shape[1])
    if rel_mask_1.max() * df.shape[1] > rel_mask_0.max() * df.shape[0]:
        index = rel_mask_1[rel_mask_1 > thresh].index
        axis = 1
    else:
        index = rel_mask_0[rel_mask_0 > thresh].index
        axis = 0
    return index, axis
