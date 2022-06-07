# -*- coding: utf-8 -*-
"""
Created on Thu Jan 20 15:34:56 2022

@author: Leon
"""
from .complements import KEYWORDS, TO_REPLACE
import numpy as np
import os
import pandas as pd
import re


def get_col(df, keys, thresh=0.5, escape=False):
    if escape:
        pat = '|'.join([re.escape(str(param)) for param in keys])
    else:
        pat = '|'.join([str(param) for param in keys])
    mask = df.apply(lambda x: x.astype('string').str.contains(pat)).sum()
    if type(thresh) == float:
        rel_mask = mask/df.count()
        return rel_mask[rel_mask >= thresh].index
    elif type(thresh) == int:
        return mask[mask >= thresh].index


def path_from_index(idx, ext):
    return os.path.join(os.getcwd(), f'{idx[0]}', f'{idx[1]}{ext}')


def get_index_from_path(path):
    path_fwd = path.replace('\\', '/')
    find = re.search(
        r'/(?P<hash>[a-f0-9]{40})', path_fwd)
    if find:
        return find.group('hash')
    else:
        return None


def make_file_index(fnames, hash_folder):
    df = pd.DataFrame(index=fnames)
    df.index.rename('fname', inplace=True)
    out = pd.concat([df], keys=[(hash_folder)],
                    names=['hash'])
    return out


def make_index_col(df, **kwargs):
    col = get_col(df, KEYWORDS['params'], **kwargs)
    if len(col) == 1:
        params = df[col]
        df.drop(col, inplace=True, axis=1)
        irange = pd.DataFrame(np.arange(df.index.size))
        df.insert(0, 'param', params.replace(
            regex=TO_REPLACE['index']).fillna(method='ffill').fillna(value=irange))


def set_index_col(data, **kwargs):
    col = get_col(data, KEYWORDS['params'], **kwargs)
    if len(col) == 1:
        index_col = data[col].copy()
        index_col.fillna(data.index.to_series(), inplace=True)
        index_col.replace(regex=TO_REPLACE['index'], inplace=True)
        data[col] = index_col
        data.set_index(*col, inplace=True)
        data.index.rename('param', inplace=True)
        return data[~data.index.duplicated()]

    elif len(col) > 1:
        return set_index_col(split_lengthwise(data, col))

    else:
        return data


def split_lengthwise(df):
    index_col = get_col(df, KEYWORDS['params'], thresh=0.1)
    if index_col.empty:
        return df
    dfs = []
    end = df.columns.size
    locs = [df.columns.get_loc(col) for col in index_col]+[end]
    for i, loc in enumerate(locs[1:]):
        sel = slice(None), slice(locs[i], loc)
        oldies = df.columns[sel[1]]
        newbs = pd.RangeIndex(0, len(oldies))
        mapper = {old: new for old, new in zip(oldies, newbs)}
        dfs.append(df.iloc[sel].rename(mapper, axis=1))
    return pd.concat(dfs, ignore_index=True)


def get_orientation(df, keys, thresh=0.5, escape=False):
    if escape:
        pat = '|'.join([re.escape(str(param)) for param in keys])
    else:
        pat = '|'.join([str(param) for param in keys])
    mask = df.apply(lambda x: x.astype('string').str.match(pat))
    rel_mask_0 = mask.sum(axis=0).divide(df.shape[0])
    rel_mask_1 = mask.sum(axis=1).divide(df.shape[1])
    if rel_mask_1.max()*df.shape[1] > rel_mask_0.max()*df.shape[0]:
        index = rel_mask_1[rel_mask_1 > thresh].index
        axis = 1
    else:
        index = rel_mask_0[rel_mask_0 > thresh].index
        axis = 0
    return index, axis
