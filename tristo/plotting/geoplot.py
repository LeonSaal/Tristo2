from re import S

import matplotlib.patches as patches
import matplotlib.patheffects as pe
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from adjustText import adjust_text
from mpl_toolkits.axes_grid1 import make_axes_locatable
from sqlalchemy import select
from sqlalchemy.orm import Session

from tristo.database import LAU_NUTS, get_vals
from tristo.database.tables import Response

from ..paths import PATH_PLOTS
from .load_geo_data import cities, country, shapes_DE, states
from .uba_plot import backgr, c_uba, cmaps

plt.style.use('uba')


def get_cities(min_pop:int, session:Session):
    filter_cities = select(LAU_NUTS.name).where(LAU_NUTS.population >= min_pop)
    city_subset = pd.read_sql(filter_cities, session.connection())
    city_subset["name"] = city_subset["name"].apply(lambda x: x.split(",")[0])
    return pd.merge(cities, city_subset, left_on="URAU_NAME", right_on="name")


def geoplot(
    param: str,
    limit: float,
    unit: str,
    df: pd.DataFrame,
    session: Session,
    agg: callable = np.mean,
    show_cities=False,
    min_pop=5e5,
    save=True,
    show_laus=False
):
    cities_subset=get_cities(min_pop=min_pop, session=session)

    val_df = pd.merge(
        shapes_DE,
        df[df.category == "> BG"].groupby("LAU").agg(agg),
        left_on="LAU_ID",
        right_on="LAU",
    )
    bg_lau = pd.merge(
        shapes_DE,
        df[df.category == "BG"].groupby("LAU").agg(agg),
        left_on="LAU_ID",
        right_on="LAU",
    )
    bg_lau = bg_lau.overlay(val_df, how='difference').dissolve()

    q_analyzed_laus = select(Response.LAU).distinct()
    analyzed_laus = pd.read_sql(q_analyzed_laus, session.connection())
    laus_df = pd.merge(
        shapes_DE,
        analyzed_laus,
        left_on="LAU_ID",
        right_on="LAU",
    ).dissolve()
    fig, ax = plt.subplots()
    geo = make_axes_locatable(ax)
    cbar = geo.append_axes("right", size="5%", pad=-0.7)

    country.plot(ax=ax, facecolor=c_uba["lb"], edgecolor=c_uba["dgrey"], lw=0.5)
    if show_laus:
        laus_df.plot(ax=ax, facecolor=c_uba["db"], edgecolor='none', alpha=0.5)

    val_df.plot(
        column="val",
        ax=ax,
        categorical=False,
        legend=True,
        cmap=cmaps["cbar"],
        cax=cbar,
        legend_kwds={"label": unit},
        vmax=limit,
        vmin=0,
    )
    bg_lau['category'] = 'BG'
    bg_lau.plot(ax=ax, column='category',cmap=cmaps["grey"], label="test", alpha=1)
    states.plot(ax=ax, lw=0.25, edgecolor=c_uba["dgrey"], facecolor="none")
    if show_cities:
        cities_subset.plot(
            ax=ax, markersize=4, edgecolor='none', facecolor=c_uba["m"], lw=0.5
        )
        texts = [
            ax.text(
                city.geometry.x,
                city.geometry.y,
                city.URAU_NAME,
                alpha=0.75,
                fontsize='x-small',
                path_effects=[pe.withStroke(linewidth=1, foreground=c_uba["lgrey"])],
            )
            for city in cities_subset.itertuples()
        ]
        adjust_text(texts, ax=ax)

    #ax.set_title(param)
    ax.get_xaxis().set_visible(False)
    ax.get_yaxis().set_visible(False)
    backgr(ax)
    bg = patches.RegularPolygon(
        (0, 0), 5, ec='black', fc=c_uba['lgrey'], label='< LOQ', linewidth = 0.25)
    if df.empty:
        label = 'Untersuchte Gemeinden'
    else:
        label = 'No data'
    laus = patches.RegularPolygon(
        (0, 0), 5, ec='none', label=label, fc='#097BAD')
    ax.legend(handles=[bg, laus], ncol=2, bbox_to_anchor=(0.5, 0.0))
    if save:
        fig.savefig(PATH_PLOTS / f"geo_{param}", pad_inches=0, bbox_inches='tight')
    return fig,ax, cbar, val_df.overlay(bg_lau, how='union').dissolve()
