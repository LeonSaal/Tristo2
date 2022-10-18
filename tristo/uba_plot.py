# -*- coding: utf-8 -*-
"""
Created on Tue Mar 29 11:21:32 2022

@author: Leon
"""

from typing import List
import matplotlib
import numpy as np
from matplotlib.rcsetup import cycler
from matplotlib.patches import Rectangle
from functools import partial, partialmethod


colors = ['#61B931', '#125D86', '#009BD5', '#007626', '#FABB00',
          '#83053C', '#CE1F5E', '#D78400', '#9D579A', '#622F63', '#EDEDEE']
cnames = ['lgr', 'db', 'lb', 'dgr', 'y', 'm', 'pink', 'or', 'lv', 'dv', 'grey']
c_uba = {name: color for name, color in zip(cnames, colors)}

cmaps = {'cbar': matplotlib.colors.LinearSegmentedColormap.from_list(
    'UBA', [matplotlib.colors.to_rgb(c_uba['dgr']), matplotlib.colors.to_rgb(c_uba['or']), matplotlib.colors.to_rgb(c_uba['m'])]),
    'red': matplotlib.colors.LinearSegmentedColormap.from_list(
    'red', [matplotlib.colors.to_rgb(c_uba['pink'])]*2),
    'grey': matplotlib.colors.LinearSegmentedColormap.from_list(
    'red', [matplotlib.colors.to_rgb(c_uba['grey'])]*2)}


rcp = {'figure.figsize': [7, 5.1],
       'axes.titlesize': 12,
       'axes.titleweight': 'bold',
       'axes.titlepad': 20,
       'axes.labelsize': 10.0,
       'axes.grid': True,
       'axes.labelweight': 'bold',
       'axes.linewidth': 0.8,
       'axes.prop_cycle': cycler('color', colors),
       'axes.xmargin': 0.05,
       'axes.ymargin': 0.05,
       'boxplot.meanprops.color': 'C4',
       'boxplot.medianprops.color': 'C0',
       'figure.dpi': 300,
       'font.family': 'Calibri',
       'font.size': 10,
       'grid.color': 'black',
       'grid.linewidth': 0.5,
       'lines.linewidth': 2.25,
       'lines.markersize': 5.0,
       'legend.loc': 'best',
       'legend.fontsize': 10.0,
       'legend.frameon': False,
       'lines.marker': ' ',
       'savefig.bbox': 'tight',
       'xtick.minor.visible': False,
       'xtick.major.size': 5,
       'ytick.major.size': 0.0
       }

matplotlib.pyplot.rcParams.update(rcp)

matplotlib.pyplot.title = partial(matplotlib.pyplot.title, loc='left')
matplotlib.pyplot.legend = partial(matplotlib.pyplot.legend, bbox_to_anchor=(
    0.5, -0.10), loc='upper center')

matplotlib.pyplot.pie = partial(matplotlib.pyplot.pie, colors=['#009BD5', '#61B931', '#007626', '#9D579A', '#83053C',
                                                               '#CE1F5E', '#D78400', '#FABB00', '#622F63', '#125D86'], counterclock=False, startangle=90)
matplotlib.axes.Axes.set_title = partialmethod(
    matplotlib.axes.Axes.set_title, loc='left')
matplotlib.axes.Axes.legend = partialmethod(matplotlib.axes.Axes.legend, bbox_to_anchor=(
    0.5, -0.10), loc='upper center')
matplotlib.axes.Axes.pie = partialmethod(matplotlib.axes.Axes.pie, colors=['#009BD5', '#61B931', '#007626', '#9D579A', '#83053C',
                                                                           '#CE1F5E', '#D78400', '#FABB00', '#622F63', '#125D86'], counterclock=False, startangle=90)


def backgr(self, hatch: str = '//////'):
    l, r = self.get_xlim()
    b, t = self.get_ylim()
    self.add_patch(Rectangle((l, b), r-l, t-b, fill=False,
                   hatch=hatch, lw=.1, ec=c_uba['grey'], zorder=0))


def subtitle(self, title: str):
    l, r = self.get_xlim()
    b, t = self.get_ylim()
    self.annotate(title, (l, t), (0, 5),
                  textcoords='offset points', size=9, weight='bold')


def pielabel(self, wedges, texts: List[str], labeldistance: float = 1.1, x_offs: float = 0.1):
    for wedge, text in zip(wedges, texts):
        angle_deg = np.mean([wedge.theta1, wedge.theta2])
        angle = np.radians(angle_deg)
        wr = wedge.r
        xyw = wr * np.cos(angle), wr * np.sin(angle)
        if -0.1*np.pi < angle < 0.1*np.pi:
            xyt = labeldistance + x_offs, xyw[1]
            self.annotate(text, xyw, xyt, va='bottom', arrowprops=dict(
                arrowstyle="-", relpos=(0, 0)), ha='right')
        elif angle > -np.pi/2:
            xyt = labeldistance * np.cos(angle) + \
                x_offs, labeldistance * np.sin(angle)
            connectionstyle = f'angle,angleA=-180,angleB={angle_deg},rad=0'
            self.annotate(text, xyw, xyt, va='bottom', arrowprops=dict(
                arrowstyle="-", relpos=(0, 0.2), connectionstyle=connectionstyle), ha='left')
        elif -0.9*np.pi > angle > -1.1*np.pi:
            xyt = -labeldistance - x_offs, xyw[1]
            self.annotate(text, xyw, xyt, va='bottom', arrowprops=dict(
                arrowstyle="-", relpos=(1, 0)), ha='right')

        else:
            xyt = labeldistance * np.cos(angle) - \
                x_offs, labeldistance * np.sin(angle)
            connectionstyle = f'angle,angleA=0,angleB={angle_deg},rad=0'
            self.annotate(text, xyw, xyt, va='bottom', arrowprops=dict(
                arrowstyle="-", relpos=(1, 0.2), connectionstyle=connectionstyle), ha='right')


matplotlib.axes.Axes.backgr = backgr
matplotlib.axes.Axes.set_subtitle = subtitle
matplotlib.axes.Axes.pielabel = pielabel
