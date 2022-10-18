# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 13:59:31 2022

@author: Leon
"""
import locale
import os

import geopandas as gpd
import pandas as pd

from .database import get_vals
from .paths import HOME
from .uba_plot import c_uba, cmaps
from .uba_plot import matplotlib as mpl

locale.setlocale(locale.LC_ALL, 'de_DE')
plt = mpl.pyplot

crs = {'proj': 'longlat', 'ellps': 'WGS84', 'datum': 'WGS84', 'no_defs': True}
os.chdir(HOME)


PATH_GEO = r'..\3_Daten\Geografisch'
PATH_SUP = r'..\3_Daten\Wasserwirtschaft'
PATH_PREPROP = 'preprocessed'

LAU_file = r'lau_de.shp'
CITIES = 'cities_de.shp'
STATES_FILE = 'states_DE.shp'
WVG_LAU = 'WVG_LAU.xlsx'

lau_de = gpd.read_file(os.path.join(PATH_GEO, PATH_PREPROP, LAU_file))
cities = gpd.read_file(os.path.join(PATH_GEO, PATH_PREPROP, CITIES))
states = gpd.read_file(os.path.join(PATH_GEO, PATH_PREPROP, STATES_FILE))

lau_wvg = pd.read_excel(os.path.join(PATH_SUP, WVG_LAU), header=0, index_col=0)
lau_wvg_shp = pd.merge(lau_de, lau_wvg, left_on='LAU_ID', right_on='LAU')


def geoplot(query: str, q: str, save: bool = False, supplied: int = 50000, save_folder=''):
    os.chdir(query)
    resp = get_vals(q)
    if resp:
        param, unit,  vals = resp
    else:
        os.chdir('..')
        print(f'No match for "{q}" found.')
        return
    bg = vals[vals.val <= 0]
    y = vals[vals.val > 0]
    conc = lau_de.merge(y.groupby('LAU').mean(),
                        left_on='LAU_ID', right_on='LAU')
    bg_lau = lau_de.merge(bg.groupby('LAU').mean(),
                          left_on='LAU_ID', right_on='LAU')
    bg_lau['bin'] = '< BG'

    fig, ax = mpl.pyplot.subplots()

    states.dissolve(by='BL').plot(
        ax=ax,
        edgecolor='none',
        linewidth=0.5,
        alpha=1,
        facecolor=c_uba['lb'])

    conc.plot(
        ax=ax,
        column='val',
        categorical=False,
        legend=True,
        legend_kwds={'label': unit, 'drawedges': False},
        cmap=cmaps['cbar'],
        alpha=1
    )

    bg_lau.plot(
        ax=ax,
        cmap=cmaps['grey'],
        label='test',
        alpha=1
    )

    # lau_wvg_shp[lau_wvg_shp['supplied'] >= supplied].dissolve(by='WVG').dissolve().plot(
    #     ax=ax,
    #     edgecolor=c_uba['db'],
    #     linewidth=0.33,
    #     facecolor='none',
    #     # legend=True,
    #     #legend_kwds={'label': 'WVG', 'drawedges': True},
    #     alpha=1
    # )
    states.dissolve(by='BL').plot(
        ax=ax,
        edgecolor='black',
        linewidth=0.3,
        alpha=1,
        facecolor='none')

    ax.backgr()
    wvg = mpl.patches.RegularPolygon(
        (0, 0), 5, ec=c_uba['db'], label=f'WVG (> {supplied:,} EW)'.replace(',', '.'), fc='none')
    bg = mpl.patches.RegularPolygon(
        (0, 0), 5, ec='black', fc=c_uba['grey'], label='< BG')
    ax.legend(handles=[bg], ncol=1, bbox_to_anchor=(0.5, 0.0))
    #ax.set_ylabel('° nördliche Breite')
    #ax.set_xlabel('° östliche Länge')
    ax.set_title(param, pad=10)
    plt.axis('off')
    if save:
        path = os.path.join(save_folder, f'geo_{param}')
        fig.savefig(path)
    os.chdir('..')
    return
