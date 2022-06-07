# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 10:10:09 2022

@author: Leon
"""

from sqlite3 import converters
from .complements import HOME, LAU_NUTS, GN250
import numpy as np
import os
import pandas as pd
import pickle
from pathlib import Path


def strip_name(name):
    if "," in name:
        return name[0 : name.find(",")]
    else:
        return name


def read_comm():
    path_dem = os.path.join(
        Path(HOME).parent, "Demografische Daten", "Gemeinden_2020.xlsx"
    )
    header = pd.read_excel(path_dem, sheet_name=1, skiprows=1, nrows=2)
    header = header.iloc[1].fillna(header.iloc[0]).dropna().values
    data = pd.read_excel(
        path_dem,
        sheet_name=1,
        names=header,
        skiprows=5,
        skipfooter=12,
        dtype={i: str for i in range(0, 7)},
        # keep_default_na=False,
        converters={"Gemeindename": lambda x: strip_name(x)},
        usecols=np.r_[0:17, 18],
    )
    ars = data["Land"].str.cat(data.loc[:, "RB":"Gem"], na_rep="")
    data = data.rename(index={idx: ars for idx, ars in zip(data.index, ars)})
    data.index.rename("ARS", inplace=True)
    return data


def read_discharge():
    path_dem = os.path.join(
        Path(HOME).parent, "Demografische Daten", "Anschlussgrad nach Kreis.xlsx"
    )
    header = pd.read_excel(path_dem, skiprows=1, nrows=3, usecols=np.r_[2:8])
    names = header.iloc[1].fillna(header.iloc[0]).values
    units = header.iloc[2].values
    header = ["ARS", "Gemeindename"] + [
        "{} / {}".format(name, unit) for name, unit in zip(names, units)
    ]
    data = pd.read_excel(
        path_dem,
        names=header,
        # index_col=0,
        skiprows=5,
        skipfooter=31,
        na_values=["-", "."],
        converters={1: lambda x: strip_name(x)},
    )
    data = data.set_index("ARS")
    data.dropna(inplace=True, thresh=4)
    int_cols = data.columns[np.r_[2:6]]
    data = data.astype(dict(zip(int_cols, [np.int] * len(int_cols))))
    return data


def read_extraction():
    path_dem = os.path.join(
        Path(HOME).parent, "Demografische Daten", "Wassergewinnung nach Art.xlsx"
    )
    header = pd.read_excel(path_dem, skiprows=2, nrows=3, usecols=np.r_[2:14])
    names = header.iloc[1].fillna(header.iloc[0]).values
    units = header.iloc[2].values
    header = ["ARS", "Gemeindename"] + [
        "{} / {}".format(name, unit) for name, unit in zip(names, units)
    ]
    data = pd.read_excel(
        path_dem,
        names=header,
        # index_col=0,
        skiprows=6,
        skipfooter=11,
        na_values=["-", ".", "..."],
        keep_default_na=False,
        converters={1: lambda x: strip_name(x)},
    )
    data.dropna(inplace=True, thresh=4)
    data = data.set_index("ARS")
    return data


def merge_dem_data(comm, disc, extr):
    other = disc.merge(extr, left_index=True, right_index=True, on="Gemeindename")
    data = comm.merge(
        other, how="left", on="Gemeindename", left_index=True, right_index=True
    )
    idxs = data.index[data.index.str.len() == 5]
    sums = []
    for idx in idxs:
        df = data[data.index.str.startswith(idx)]
        sums += [pd.DataFrame([df.iloc[:, np.r_[8:12,]].sum()], index=[idx])]
    sums = pd.concat(sums)
    data.update(sums)
    return data.convert_dtypes()


def save_dem_data(data, name):
    dem_path = os.path.join(Path(HOME).parent, "Demografische Daten", name + ".xlsx")
    data.to_excel(dem_path)
    with open(name + ".pkl", "wb") as output:
        pickle.dump(data, output, pickle.HIGHEST_PROTOCOL)


def read_dem_data(name):
    with open(name + ".pkl", "rb") as inp:
        data = pickle.load(inp)
    return data


def get_communities(dem_data):
    data_sort = dem_data.copy().sort_values(
        by=["insgesamt"], ascending=False, na_position="last"
    )
    data_sort = data_sort["Gemeindename"][data_sort.index.str.len() == 12]
    return data_sort.str.replace(r"([.]$)|(\s*/.+$)", "", regex=True)


def get_districts(ARS):
    districts = GN250["NAME"][GN250["ARS"] == ARS]
    return set(districts.to_list())


def get_districts_from_comm(comm):
    districts = GN250["NAME"][GN250["GEMEINDE"] == comm]
    if districts.empty:
        return {comm}
    else:
        return set(districts.to_list())


def get_comm_from_LAU(LAU):
    if LAU != "":
        for year, df in LAU_NUTS.items():
            name = (
                df.iloc[:, 2][df.iloc[:, 1] == int(LAU)]
                .replace(r"\s?[,/].*\Z", "", regex=True)
                .to_list()
            )
            print(name)
            if name != []:
                break
    return name[0]


def get_from_LAU(LAU: int):
    if LAU != "":
        for year, df in LAU_NUTS.items():
            LAU_df = df[df.LAU == int(LAU)]
            if not LAU_df.empty:
                break
    return LAU_df.squeeze().T.to_dict()
