import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.colors import Normalize
from matplotlib.patches import Polygon
from matplotlib.collections import PatchCollection
from scipy.stats import norm
from scipy.optimize import curve_fit
from math import pi

import filtering
import correction

from config import (
    FILTER_CONF, CORRECTION_CONF,
    TEMP_DIFF_SAMPLE_SAVING, TEMP_DIFF_SAMPLE_FILE,
    INDICATOR_THRESHOLD
)


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

    trail_length = int(CORRECTION_CONF['window'] / (t[1]-t[0]))
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


def x_interval_polygon(x1, x2, ax):
    y1, y2 = ax.get_ylim()
    return Polygon(np.array([
        [x1, y1],
        [x2, y1],
        [x2, y2],
        [x1, y2],
    ]))


def create_patch_collection_from_mask(t, mask, ax):
    intervals = filtering.smooth_intervals(filtering.intervals_from_mask(mask))
    polygons = []
    for start, end in intervals:
        polygons.append(x_interval_polygon(t[start], t[end-1], ax))
    return PatchCollection(polygons, alpha=0.15, color='tab:red', edgecolor=None)


def plot_filtering(df, fig=None):
    if fig is None:
        fig = plt.figure()
    fig.clf()

    n_plots = 3

    ax_H = fig.add_subplot(n_plots, 1, 1)
    ax_P = ax_H.twinx()
    ax_H_flt = fig.add_subplot(n_plots, 1, 2, sharex=ax_H)
    # ax_P_flt = ax_H_flt.twinx()
    ax_P_flt = ax_H_flt
    ax_hist = fig.add_subplot(n_plots, 1, 3, sharex=ax_H)

    t_H_raw, H_raw = filtering.extract_time_series(df.loc[:, ['datetime', 'H']])
    t_P_raw, P_raw = filtering.extract_time_series(df.loc[:, ['datetime', 'H_P']])

    t, (H, P) = filtering.to_uniform_grid(df.loc[:, ['datetime', 'H', 'H_P']])
    H_flt = filtering.filter_array(t, H)
    P_flt = filtering.filter_array(t, P)

    # H_flt_norm = filtering.normalize_array(H_flt)
    # P_flt_norm = filtering.normalize_array(P_flt)
    H_flt_norm = H_flt
    P_flt_norm = P_flt

    rawdiff = H_flt_norm - P_flt_norm
    if TEMP_DIFF_SAMPLE_SAVING:
        with open(TEMP_DIFF_SAMPLE_FILE, 'a') as f:
            f.writelines([str(x)+'\n' for x in rawdiff])

    t_ind, indicator = correction.indicator(t, H_flt_norm, P_flt_norm)
    indicator_mask = np.greater(indicator, INDICATOR_THRESHOLD)

    #### Plotting ####

    ax_H_flt.set_xlabel('time, s')

    H_color = 'tab:blue'
    H_flt_color = 'tab:red'
    P_color = 'tab:green'
    P_flt_color = 'tab:purple'

    ax_H.tick_params(axis='y', labelcolor=H_color)
    ax_H.set_ylabel('H, m', color=H_color)
    ax_H.plot(t, H, color=H_color, label='GPS altitude')
    
    ax_P.tick_params(axis='y', labelcolor=P_color)
    ax_P.set_ylabel('-P', color=P_color)
    ax_P.plot(t, P, color=P_color, label='Barometric altitude')

    handles, labels = [], []
    for ax in [ax_P, ax_H]:
        ax_h_l = ax.get_legend_handles_labels()
        handles.extend(ax_h_l[0])
        labels.extend(ax_h_l[1])

    ax_H.legend(handles, labels)
    
    # ax_H.plot(t_H_raw, H_raw, color=H_color, marker='.', linestyle='none')
    # ax_H.plot(t, H-H_flt, color=H_flt_color)

    # ax_P.tick_params(axis='y', labelcolor=P_color)
    # ax_P.set_ylabel('-P', color=P_color)
    # ax_P.plot(t, P, color=P_color)
    # ax_P.plot(t_P_raw, P_raw, color=P_color, marker='.', linestyle='none')
    # ax_P.plot(t, P-P_flt, color=P_flt_color)

    ind_polygons = create_patch_collection_from_mask(t_ind, indicator_mask, ax_H)
    if ind_polygons:
        ax_H.add_collection(ind_polygons)

    ax_H_flt.tick_params(axis='y')
    ax_H_flt.set_ylabel('H filtered, m')
    ax_H_flt.plot(t, H_flt_norm, color=H_color, label='GPS altitude (HPF)')

    # ax_P_flt.tick_params(axis='y', labelcolor=P_flt_color)
    # ax_P_flt.set_ylabel('P_highpass', color=P_flt_color)
    ax_H_flt.plot(t, P_flt_norm, color=P_color, label='Barometric altitude (HPF)')

    ax_hist.set_ylabel('\Delta H filtered, m')
    ax_hist.plot(t_ind, indicator, color='tab:purple', label='Moving-average-smoothed absolute delta between GPS and barometric altitudes')
    ax_hist.axhline(INDICATOR_THRESHOLD, color='tab:red', label='Correction threshold')
    # ax_hist.hist(diff, bins=50, density=True)
    # ax_hist.vlines([q_hi, q_low], ax_hist.get_ylim()[0], ax_hist.get_ylim()[1])

    ax_hist.set_xlabel('Time since flight start, sec')

    ax_H.set_xlim(20000, 25000)

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
    ax2.plot(*correction.moving_correlation(t, H_flt, P_flt), color=corr_color)

    plt.show()


def plot_delta_distribution():

    def gaussian(x, x0, sigma):
        return (1/(sigma*np.sqrt(2*pi))) * np.exp(-(x - x0) ** 2 / (2 * sigma ** 2))

    def gauss_fit(x, y):
        mean_estimate = sum(x * y) / sum(y)
        sigma_estimate = np.sqrt(sum(y * (x - mean_estimate) ** 2) / sum(y))
        popt, pcov = curve_fit(gaussian, x, y, p0=[mean_estimate, sigma_estimate])
        return popt

    with open('temp.txt') as f:
        data = []
        for l in f:
            data.append(float(l))

    data = np.array(data)
    n, bins, _ = plt.hist(data, 1000, density=True)
    bincenters = (bins[:-1] + bins[1:]) / 2

    x0, sigma = gauss_fit(bincenters, n)
    threshold = x0 + 3*sigma

    plt.plot(bincenters, n, 'b.')

    pdf_x = np.linspace(bins[0], bins[-1], 1000)
    plt.plot(pdf_x, gaussian(pdf_x, x0, sigma), '-r')

    ax = plt.gca()
    ax.axvline(x=threshold, color='r')
    ax.axvline(color='grey')

    print(np.mean(data))
    print(np.std(data))
    plt.show()