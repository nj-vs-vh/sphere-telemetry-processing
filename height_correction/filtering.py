from scipy import signal
from scipy.interpolate import interp1d

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from config import INTERP_TYPE


def normalize_array(a: np.ndarray) -> np.ndarray:
    return a / np.std(a)


def timedelta2sec(dt: np.timedelta64):
    return dt / np.timedelta64(10**9, 'ns')


def to_uniform_grid(x, *y):
    """Interpolate signals defined on arbitrary grid to uniform

    Uniform grid is inferred from x to have the same median step.

    Args:
        x  (nd.ndarray | pd.Series): original time stamps in numeric or timedelta64(ns)
        *y (nd.ndarray | pd.Series): arbitrary number of time series

    OR  x  (pd.DataFrame): first column is treated as timestamps, others — as time series
    """
    if not y:
        df = x
        x = df.iloc[:, 0]
        y = []
        for i in range(1, len(df.columns)):
            y.append(df.iloc[:, i].to_numpy())
    numpy_args = []
    for s in (x, *y):
        if isinstance(s, pd.Series):
            numpy_args.append(s.to_numpy())
        else:
            numpy_args.append(s)
    x, *ys = numpy_args
    x = x - x[0]
    if x.dtype == np.dtype('<m8[ns]'):
        x = timedelta2sec(x)
    else:
        try:
            x = x.astype(np.dtype('float64'))
        except ValueError:
            raise ValueError('Time stamps must be timedelta64 or be convertible to float64')
    x_step = np.median(np.diff(x))
    x_start = x[0]
    x_stop = x[-1]
    x_grid = np.arange(x_start, x_stop, x_step)

    ys_grid = []
    for y in ys:
        mask = np.logical_not(np.isnan(y))
        y_grid = interp1d(
            x[mask], y[mask], kind=INTERP_TYPE, fill_value="extrapolate"
        )(x_grid)
        ys_grid.append(y_grid)

    return x_grid, ys_grid


def create_butterworth_hpf(cutoff_hz, slope_db_oct, timestamps, filter_out = 'sos'):
    fs_hz = 1 / timestamps[1]-timestamps[0]
    nyq_hz = 0.5 * fs_hz
    wp = cutoff_hz / nyq_hz # lower edge of the passband
    k = 3 # more or less arbitrary, >=1
    ws = wp / k
    gpass = 1
    gstop = slope_db_oct * k / 2 # /2 is purely empiric. don't judge.
    N, Wn = signal.buttord(wp, ws, gpass, gstop)
    # print('butterworth\'s filter N =', N)
    return signal.butter(N, Wn, btype='highpass', output=filter_out)


def filter_array(x, sos):
    return signal.sosfiltfilt(sos, x)


def plot_filter_response(cutoff_hz, slope_db_oct, fs_hz):
    b, a = create_butterworth_hpf(cutoff_hz, slope_db_oct, fs_hz, filter_out='ba')
    w, h = signal.freqz(b, a, fs=fs_hz, worN=np.logspace(-4, -2, 50))
    plt.semilogx(w, 20 * np.log10(abs(h)))
    plt.title('Butterworth filter frequency response')
    plt.xlabel('Frequency [radians / second]')
    plt.ylabel('Amplitude [dB]')
    plt.margins(0, 0.1)
    plt.grid(which='both', axis='both')
    plt.axvline(cutoff_hz, color='green') # cutoff frequency
    plt.show()