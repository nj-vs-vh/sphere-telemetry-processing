import numpy as np
from scipy.stats import pearsonr
from scipy.optimize import curve_fit
from scipy.interpolate import interp1d
from math import copysign

import matplotlib.pyplot as plt

import pandas as pd

from config import CORRECTION_CONF, INDICATOR_THRESHOLD

import filtering


def moving_correlation(t, s1, s2):
    # sec -> t bin
    window = int(CORRECTION_CONF["correlation_window"] / (t[1] - t[0]))
    t_corr = t[window:]
    r_corr = np.zeros_like(t_corr, dtype=np.dtype("float64"))
    for i in range(window, len(t)):
        r_corr[i - window] = pearsonr(s1[i - window : i], s2[i - window : i])[0]
    return t_corr, r_corr


def moving_average(a, n):
    ret = np.cumsum(a, dtype=float)
    ret[n:] = ret[n:] - ret[:-n]
    return ret[n - 1 :] / n


def moving_average_smoothing(x, y, n):
    x_step = x[1] - x[0]
    x_extended = np.linspace(x[0] - x_step * n / 2, x[-1] + x_step * n / 2, len(x) + n)
    x_ma = moving_average(x_extended, n)
    y_ma = moving_average(
        interp1d(x, y, kind="linear", bounds_error=False, fill_value=(y[0], y[-1]))(
            x_extended
        ),
        n,
    )
    return x_ma, y_ma


def indicator(t, y1, y2):
    window_size = int(CORRECTION_CONF["ma_window"] / (t[1] - t[0]))
    diff = y2 - y1
    center = np.median(diff)
    # center = 0
    one_sided_diff = np.abs(diff - center)
    return moving_average_smoothing(t, one_sided_diff, n=window_size)


def process_interval(df: pd.DataFrame):
    t, (_, H_P) = filtering.to_uniform_grid(df.loc[:, ["datetime", "H", "H_P"]])

    # for fitting
    _, (H, P) = filtering.to_uniform_grid(df.loc[:, ["datetime", "H", "P_hpa1"]])

    n_fittings = 5
    for i in range(n_fittings):
        print(".", end="")

        H_P_flt = filtering.filter_array(t, H_P)
        H_flt = filtering.filter_array(t, H)

        t_ind, ind = indicator(t, H_flt, H_P_flt)
        indicator_mask = np.greater(ind, INDICATOR_THRESHOLD)

        indicator_mask = interp1d(
            t_ind, indicator_mask, kind="nearest", fill_value=False, bounds_error=False
        )(t).astype("bool")

        last_end = -1
        for start, end in filtering.smooth_intervals(
            filtering.intervals_from_mask(indicator_mask)
        ):
            if start > last_end:
                last_end, corrected_H = barometric_height_correction(
                    t, H, P, indicator_mask, start, end
                )
                H[start:end] = corrected_H

    print("")

    t_df = df["datetime"].to_numpy()
    t_df = t_df - t_df[0]
    H_to_df = interp1d(
        t, H, kind="linear", fill_value="extrapolate", bounds_error=False
    )(filtering.timedelta2sec(t_df))

    df.insert(len(df.columns), column="H_corrected", value=H_to_df)


def find_adjasent_interval(direction, window, mask, istart):
    def window_limits(i):
        if direction > 0:
            start = i
            end = min(i + window, len(mask))
        else:
            start = max(i - window + 1, 0)
            end = i + 1
        return start, end

    def get_window(i):
        start, end = window_limits(i)
        return mask[start:end]

    def on_edge(i):
        if direction > 0:
            return i == len(mask) - 1
        else:
            return i == 0

    i = istart
    while any(get_window(i)) and not on_edge(i):
        i = int(i + copysign(1, direction))

    return window_limits(i)


def barometric_height_correction(t, H, P, mask, imin, imax):
    window_size = int(CORRECTION_CONF["fitting_window"] / (t[1] - t[0]))
    left = find_adjasent_interval(-1, window_size, mask, imin)
    right = find_adjasent_interval(1, window_size, mask, imax - 1)

    def barometric_func(P, P0, a):
        return H_points[0] - a * np.log(P / P0)

    def barometric_fit(P, H, weights):
        P0_est = P_points[0]
        a_est = 8400
        popt, _ = curve_fit(
            barometric_func, P, H, p0=[P0_est, a_est], sigma=weights ** (-2)
        )
        return popt

    def prepare_data_for_fit(lims):
        start, end = lims
        P_data = P[start:end]
        H_data = H[start:end]
        return P_data, H_data, (1 / (end - start)) * np.ones_like(P_data)

    P_left, H_left, weights_left = prepare_data_for_fit(left)
    P_right, H_right, weights_right = prepare_data_for_fit(right)

    P_points = np.array((*P_left, *P_right))
    H_points = np.array((*H_left, *H_right))
    weights = np.array((*weights_left, *weights_right))

    PLOTTING = True

    if PLOTTING:
        plt.plot(P_points, H_points, "b.")
        plt.plot(P[imin:imax], H[imin:imax], "rx")

    try:
        P0, a = barometric_fit(P_points, H_points, weights)
        if PLOTTING:
            smooth_x = np.linspace(np.min(P_points), np.max(P_points), 1000)
            plt.plot(smooth_x, barometric_func(smooth_x, P0, a), "-r")
        H_corrected = barometric_func(P[imin:imax], P0, a)
    except Exception:
        H_corrected = H[imin:imax]

    if PLOTTING:
        plt.show()

    return right[1], H_corrected
