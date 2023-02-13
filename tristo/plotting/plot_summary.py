
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import ScalarFormatter
from sqlalchemy.orm import Session

from tristo.database import summary
from tristo.paths import PATH_PLOTS

trans={'Nitrat': 'Nitrate',
  'Elektrische Leitfähigkeit': 'Conductivity',
  'Sulfat': 'Sulfate',
  'Natrium': 'Sodium',
  'Chlorid': 'Chloride',
  'Fluorid': 'Fluoride',
  'Eisen': 'Iron',
  'Nitrit': 'Nitrite',
  'Mangan': 'Manganese',
  'Ammonium': 'Ammoniume',
  'Atrazin': 'Atrazine',
  'Aluminium': 'Aluminium',
  'Trübung': 'Turbidity',
  'Blei': 'Lead',
  'Quecksilber': 'Mercury',
  'Kupfer': 'Copper',
  'Nickel': 'Nickel',
  'Chrom': 'Chromium',
  'Organisch gebundener Kohlenstoff (TOC)': 'Total organic carbon',
  'Uran': 'Uranium',
  'Arsen': 'Arsenic',
  'Cyanid': 'Cyanide',
  'Cadmium': 'Cadmium',
  'Selen': 'Selenium'}


def plot_summary(session:Session):
    summa = summary(session=session)
    fig, ax = plt.subplots(figsize = (6.7,2))
    df = summa['Currentness of Reports:']
    df.sort_values('Year').plot(kind='barh', x = 'Year', y='Counts', ylabel = 'Year', xlabel = 'Number of reports', legend=False, ax=ax)# summa['Currentness of Reports:'].
    ax.grid(axis='y')
    fig.savefig(PATH_PLOTS / 'reports_per_year')

    fig, ax = plt.subplots(figsize = (6.7,2))
    df = summa['Supplier has n_th result:']
    df.sort_values('N', ascending = False).plot(x='N', y='Fraction', kind = 'barh', legend =False, xlabel = 'Fraction of supplier and communitypages in %', ylabel = 'Position of result', ax =ax)
    ax.grid(axis='y')
    fig.savefig(PATH_PLOTS / 'position_result')

    fig, ax = plt.subplots(figsize = (3.35,4))
    df = summa['Parameter Count:']
    df=df.replace(regex=trans)
    df.nlargest(20,'Count').sort_values('Count', ascending=True).plot(x='param',y='Count', kind='barh', ylabel='', legend=False, xlabel = 'Number of observations', ax=ax)
    ax.grid(axis='y')
    fig.savefig(PATH_PLOTS / 'frequent_param')

    fig, ax = plt.subplots(figsize = (6.7,2))
    ax2 = plt.twinx(ax)
    df = summa['n param per Report ungrouped:']
    bins = [i for i in range(0,300,10)]#+[200,250, 300]
    df.plot(y='N params', kind='hist', bins=bins, ax=ax2, legend=False, xlim=(0, 300), cumulative=True, density=True, color='C1', alpha=0.6)
    df.plot(y='N params', kind='hist', bins=bins, ax=ax, legend=False, xlim=(0, 300))
    ax.set_ylim(0,250)
    ax2.set_ylim(0,1)
    ax.set_ylabel('Frequency')
    ax2.set_ylabel('Cumulated Frequency')
    ax2.set_yticklabels([f'{i:.0%}' for i in np.linspace(0,1,6)])
    ax.set_xlabel('Number of parameters')
    ax1_h,_= ax.get_legend_handles_labels()
    ax2_h,_ = ax2.get_legend_handles_labels()
    plt.legend(ax1_h+ax2_h, ['Frequency', 'Cumulated Frequency'], loc= 'lower center', ncol=2, bbox_to_anchor=(0.5,-0.4))
    fig.savefig(PATH_PLOTS / 'hist_number_param')

    fig, ax = plt.subplots(figsize = (6.7,2))
    df = summa['Parameter Count:']
    df.plot(kind='hist', bins = range(0,4000,100), figsize = (3.35,2),logy=True, ax = ax)
    ax.grid(axis='x')
    ax.yaxis.set_major_formatter(ScalarFormatter())
    ax.set_xlabel('Number of observations per parameter')
    ax.set_ylabel('$\log$ Number of parameters')
    ax.get_legend().remove()
    fig.savefig(PATH_PLOTS / 'observ_per_param')
    
