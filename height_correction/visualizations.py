import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.colors import Normalize

import filtering
import correlations

from config import FILTER_CONF, CORRELATION_CONF


def animate_filtered_scatter(df, fig=None):
    if fig is None:
        fig = plt.figure()
    fig.clf()
    ax = fig.add_subplot()
    ax.axis('equal')

    t, (H, P) = filtering.to_uniform_grid(df.loc[:, ['datetime', 'H', 'P_hpa1']])

    sos = filtering.create_butterworth_hpf(FILTER_CONF['cutoff'], FILTER_CONF['slope'], t)
    H_flt = filtering.normalize_array(filtering.filter_array(H, sos))
    P_flt = filtering.normalize_array(filtering.filter_array(-P, sos))

    trail_length = int(CORRELATION_CONF['window'] / (t[1]-t[0]))
    scmap = cm.ScalarMappable(norm=Normalize(0, trail_length), cmap=plt.get_cmap('cool'))
    trail_colors = scmap.to_rgba(range(trail_length))
    range_sigma = 5
    for i in range(trail_length, len(H_flt)):
        plt.cla()
        plt.scatter(H_flt[i-trail_length:i], P_flt[i-trail_length:i], c=trail_colors)
        ax.set_ylim(-range_sigma, range_sigma)
        ax.set_xlim(-range_sigma, range_sigma)
        plt.pause(0.001)
    plt.show()


def plot_filtering(df, fig=None):
    if fig is None:
        fig = plt.figure()
    fig.clf()

    ax_H = fig.add_subplot(2, 1, 1)
    ax_P = ax_H.twinx()
    ax_H_flt = fig.add_subplot(2, 1, 2, sharex=ax_H)
    ax_P_flt = ax_H_flt.twinx()

    t, (H, P) = filtering.to_uniform_grid(df.loc[:, ['datetime', 'H', 'P_hpa1']])
    P = -P
    sos = filtering.create_butterworth_hpf(FILTER_CONF['cutoff'], FILTER_CONF['slope'], t)
    H_flt = filtering.filter_array(H, sos)
    P_flt = filtering.filter_array(P, sos)

    H_flt_norm = filtering.normalize_array(H_flt)
    P_flt_norm = filtering.normalize_array(P_flt)

    ax_H_flt.set_xlabel('time, s')

    H_color = 'tab:blue'
    H_flt_color = 'tab:red'
    P_color = 'tab:green'
    P_flt_color = 'tab:purple'

    ax_H.tick_params(axis='y', labelcolor=H_color)
    ax_H.set_ylabel('H', color=H_color)
    ax_H.plot(t, H, color=H_color)
    ax_H.plot(t, H-H_flt, color=H_flt_color)

    ax_P.tick_params(axis='y', labelcolor=P_color)
    ax_P.set_ylabel('-P', color=P_color)
    ax_P.plot(t, P, color=P_color)
    ax_P.plot(t, P-P_flt, color=P_flt_color)

    ax_H_flt.tick_params(axis='y', labelcolor=H_flt_color)
    ax_H_flt.set_ylabel('H_highpass', color=H_flt_color)
    ax_H_flt.plot(t, H_flt_norm, color=H_flt_color)

    ax_P_flt.tick_params(axis='y', labelcolor=P_flt_color)
    ax_P_flt.set_ylabel('P_highpass', color=P_flt_color)
    ax_P_flt.plot(t, P_flt_norm, color=P_flt_color)

    plt.show()


def plot_correlation(df, fig=None):
    if fig is None:
        fig = plt.figure()
    fig.clf()
    ax = fig.add_subplot()
    ax2 = ax.twinx()

    t, (H, P) = filtering.to_uniform_grid(df.loc[:, ['datetime', 'H', 'P_hpa1']])
    sos = filtering.create_butterworth_hpf(FILTER_CONF['cutoff'], FILTER_CONF['slope'], t)
    H_flt = filtering.normalize_array(filtering.filter_array(H, sos))
    P_flt = filtering.normalize_array(filtering.filter_array(-P, sos))

    ax.set_xlabel('time, s')

    H_color = 'tab:blue'
    ax.tick_params(axis='y', labelcolor=H_color)
    ax.set_ylabel('H', color=H_color)
    ax.plot(t, H, color=H_color)

    corr_color = 'tab:red'
    ax2.tick_params(axis='y', labelcolor=corr_color)
    ax2.set_ylabel('correlation', color=corr_color)
    ax2.plot(*correlations.moving_correlation(t, H_flt, P_flt), color=corr_color)

    plt.show()