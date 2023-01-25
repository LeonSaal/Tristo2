
import matplotlib.pyplot as plt
from sqlalchemy.orm import Session

from tristo.database import summary
from tristo.paths import PATH_PLOTS

plt.style.use('uba')
import numpy as np

DE = {'n_reports':'Anzahl Berichte',
'year':'Jahr',
'frac_supp':'Anteil der Versorger- und Gemeinde-Seiten in %',
'#_res':'Nummer des Ergebnisses',
'n_observed':'Anzahl Beobachtungen',
'freq':'Frequenz',
'freq_cum':'Kumulierte Häufigkeit',
'#_param':'Anzahl Parameter',
'n_observed_per_param':'Anzahl Beobachtungen je Parameter'}

EN = {'n_reports':'number of reports',
'year':'yeear',
'frac_supp':'Fraction of supplier and community pages in %',
'#_res':'Position of result',
'n_observed':'Number of observations',
'freq':'Frequency',
'freq_cum':'Cumulated frequency',
'#_param':'Number of parameters',
'n_observed_per_param':'Number of observations pr parameter'}

trans={'DE':DE, 'EN':EN}


def plot_summary(session:Session, lang = 'EN'):
    summa = summary(session=session)
    fig, ax = plt.subplots(figsize = (7,2.5))
    df = summa['Currentness of Reports:']
    df.sort_values('date').plot(kind='barh', x = 'date', y='Count', ylabel = trans[lang]['year'], xlabel = trans[lang]['n_reports'], legend=False, ax=ax)# summa['Currentness of Reports:'].
    ax.grid(axis='y')
    fig.savefig(PATH_PLOTS / 'Berichte_pro_jahr.png', dpi=300)

    fig, ax = plt.subplots(figsize = (7,2))
    df = summa['Supplier has n_th result:']
    df.sort_values('N', ascending = False).plot(x='N', y='Fraction', kind = 'barh', legend =False, xlabel = trans[lang]['frac_supp'], ylabel = trans[lang]['#_res'], ax =ax)
    ax.grid(axis='y')
    fig.savefig(PATH_PLOTS / 'position_suchergebnis.png', dpi=300)

    fig, ax = plt.subplots(figsize = (7,4))
    df = summa['Parameter Count:']
    df.nlargest(20,'Count').sort_values('Count', ascending=True).plot(x='param',y='Count', kind='barh', ylabel='', legend=False, xlabel = trans[lang]['n_observed'], ax=ax)
    ax.grid(axis='y')
    fig.savefig(PATH_PLOTS / 'häufigste_param.png', dpi=300)

    fig, ax = plt.subplots(figsize = (7,2.5))
    ax2 = plt.twinx(ax)
    df = summa['n param per Report ungrouped:']
    bins = [i for i in range(0,300,10)]#+[200,250, 300]
    df.plot(y='N params', kind='hist', bins=bins, ax=ax2, legend=False, xlim=(0, 300), cumulative=True, density=True, color='C2', alpha=0.5)
    df.plot(y='N params', kind='hist', bins=bins, ax=ax, legend=False, xlim=(0, 300))
    ax.set_ylim(0,250)
    ax2.set_ylim(0,1)
    ax.set_ylabel(trans[lang]['freq'])
    ax2.set_ylabel(trans[lang]['freq_cum'])
    ax2.set_yticklabels([f'{i:.0%}' for i in np.linspace(0,1,6)])
    ax.set_xlabel(trans[lang]['#_param'])
    ax1_h,_= ax.get_legend_handles_labels()
    ax2_h,_ = ax2.get_legend_handles_labels()
    plt.legend(ax1_h+ax2_h, [trans[lang]['freq'], trans[lang]['freq_cum']], loc= 'lower center', ncol=2, bbox_to_anchor=(0.5,-0.4))
    fig.savefig(PATH_PLOTS / 'anzahl_param.png', dpi=300)

    fig, ax = plt.subplots(figsize = (7,2.5))
    df = summa['Parameter Count:']
    df.plot(kind='hist', bins = range(0,4000,100), figsize = (7,2), ax = ax)
    ax.grid(axis='x')
    ax.set_xlabel(trans[lang]['n_observed_per_param'])
    ax.get_legend().remove()
    fig.savefig(PATH_PLOTS / 'observ_per_param.png', dpi=300)
    
