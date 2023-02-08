import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib import ticker

formatter = ticker.ScalarFormatter(useMathText=True)
formatter.set_scientific(True) 
formatter.set_powerlimits((-2,2)) 
from matplotlib.offsetbox import AnchoredText


def violinplot(param:str, limit:float, unit:str, df:pd.DataFrame, ax=plt.Axes, legend=''):
    df = df.query('category in ["BG", "> BG"]')
    df = df.drop_duplicates('id')
    df = df.replace(regex={"BG":'LOQ'})
    categrories = ["LOQ", "> LOQ"]
    for category in categrories:
        if category not in df.category.unique():
            df = pd.concat([df, pd.DataFrame(['> LOQ'], columns=['category'])])
    df['Parameter']=f'{param}'
    sns.violinplot(data=df, x='Parameter', y='val' , hue='category', split=True, inner='stick', ax=ax, scale='count', cut=0, linewidth=.5, hue_order=categrories, palette=['C0','C2'])
    lim_line = ax.axhline(limit, ls='dashed', c='C5')
    part_lim_line = ax.axhline(limit*0.3, ls='dashed', c='C7', xmax=0.5)
    ax.set_ylabel('')
    ax.set_xlabel('')
    ax.set_ylim(0,None)
    ax.yaxis.set_major_formatter(formatter) 
    lgd = ax.get_legend_handles_labels()
    lgd[0].extend([lim_line,part_lim_line])
    lgd[1].extend([legend, f'30 % {legend}'])
    ax.get_legend().remove()
    at = AnchoredText(f'N = {df.index.size}', 'lower center', borderpad=-5)
    ax.add_artist(at)
    return lgd
