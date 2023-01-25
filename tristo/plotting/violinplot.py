import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib import ticker

formatter = ticker.ScalarFormatter(useMathText=True)
formatter.set_scientific(True) 
formatter.set_powerlimits((-2,2)) 
from matplotlib.offsetbox import AnchoredText

plt.style.use('uba')

def violinplot(param:str, limit:float, unit:str, df:pd.DataFrame, ax=plt.Axes):
    df = df.query('category in ["BG", "> BG"]')
    df = df.drop_duplicates('id')
    categrories = ["BG", "> BG"]
    for category in categrories:
        if category not in df.category.unique():
            df = pd.concat([df, pd.DataFrame(['> BG'], columns=['category'])])
    df['Parameter']=f'{param}'
    sns.violinplot(data=df, x='Parameter', y='val' , hue='category', split=True, inner='stick', ax=ax, scale='count', cut=0, linewidth=.5, hue_order=categrories, palette=['C0','C2'])
    lim_line = ax.axhline(limit, ls='dashed', c='C5')
    part_lim_line = ax.axhline(limit*0.7, ls='dashed', c='C7')
    ax.set_ylabel('')
    ax.set_xlabel('')
    ax.set_ylim(0,None)
    ax.yaxis.set_major_formatter(formatter) 
    lgd = ax.get_legend_handles_labels()
    lgd[0].extend([lim_line,part_lim_line])
    lgd[1].extend(['Grenz-/Richtwert', '70 % Grenz-/Richtwert'])
    ax.get_legend().remove()
    at = AnchoredText(f'N = {df.index.size}', 'lower center', borderpad=-5, prop={'size':'x-small'})
    ax.add_artist(at)
    return lgd