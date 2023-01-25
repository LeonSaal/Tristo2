import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from ..paths import PATH_PLOTS
from .uba_plot import backgr, c_uba


def boxplot_param(param_name, limit, unit, df:pd.DataFrame, save=True):
    xmax = limit
    if not limit or isinstance(limit, str):
        xmax = df.val.max()
    vals = df.query('category == "> BG"').val
    bg = df.query('category == "< BG"').val
    outliers = df.query('category == "OUTLIER"').val

    fig, ax =plt.subplots(figsize=(plt.rcParams["figure.figsize"][0],1))
    ax.boxplot(vals,vert=False,patch_artist=True, boxprops={"facecolor": "C3"},)
    ax.boxplot(bg, positions=[2], vert=False, patch_artist=True,boxprops={"facecolor": "C2"},)
    if limit and limit!=np.nan:
        print(param_name, limit)
        ax.set_xlim(0, 1.1 * xmax)
        ax.axvline(limit,c=c_uba['pink'], lw=2, ls='dashed')
        ax.axvline(0.7*limit, c=c_uba['or'], lw=2, ls='dashed')
    else:
        if unit == "µg/l":
            ax.set_xlim(0, 5)
            

    ax.set_yticks([1, 2], labels=[f"> BG: N = {vals.index.size}", f"< BG: N={bg.index.size}"])
    ax.set_xlabel(f"{unit}")
    ax.set_title(param_name)
    backgr(ax)
    plt.show()
    total_len = len(df)
    if total_len == 0:
        total_len = 1
    print(
        "; ".join(
            [
                f"{name}: N={x.index.size} ({x.index.size/total_len:.2%})"
                for name, x in zip(
                    ["> BG", "< BG", "not validated"], [vals, bg, outliers]
                )
            ]
        )
    )
    if save:
        fig.savefig(PATH_PLOTS / f'box_{param_name}.png')
