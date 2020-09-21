from scipy import signal
from scipy.interpolate import interp1d

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from config import INTERP_TYPE, FILTER_CONF


def intervals_from_mask(mask, polarity=True):
    """
    Convert bool mask to slices with given polarity

    Ex.: [0, 0, 0, 1, 1, 1, 0, 0, 1, 0, 0, 1, 1] -> [(3,6), (8,9), (11,13)]

    Resulting tuples are ready for slicing, i.e.
    for i, j in intervals_from_mask(mask):
        assert mask[i:j].all() == True
    """
    if len(mask)==0:
        return []
    if mask[0]==polarity:
        start_idx = 0
    else:
        start_idx = 1
    intervalends = np.nonzero(np.diff(mask) != False)[0]
    intervalends = [-1] + list(intervalends) + [len(mask)-1]
    return [
        (intervalends[i]+1, intervalends[i+1]+1)
        for i in range(start_idx, len(intervalends)-1, 2)
    ]


def smooth_intervals(intervals, smallest_gap=30, smallest_length=20):

    def merge_interval_with_next(intervals, i):
        before = intervals[:i]
        merged = (intervals[i][0], intervals[i+1][1])
        after = intervals[i+2:] if i+2<len(intervals) else []
        return [*before, merged, *after]

    while True:
        gaps = [intervals[i+1][0]-intervals[i][1] for i in range(len(intervals)-1)]
        if len(gaps)==0 or min(gaps) > smallest_gap:
            break
        else:
            intervals = merge_interval_with_next(intervals, np.argmin(gaps))

    while True:
        lengths = [i[1]-i[0] for i in intervals]
        if len(lengths)==0 or min(lengths)>smallest_length:
            break
        else:
            intervals.pop(np.argmin(lengths))

    return intervals


def normalize_array(a: np.ndarray) -> np.ndarray:
    return a / np.std(a)


def timedelta2sec(dt: np.timedelta64):
    return dt / np.timedelta64(10**9, 'ns')


def extract_time_series(df):
    """Convert dataframe (2 columns) to 2 numpy arrays"""
    t = df.iloc[:,0].to_numpy()
    t = t-t[0]
    y = df.iloc[:,1].to_numpy()
    mask = np.logical_not(np.isnan(y))
    return (
        timedelta2sec(t[mask]),
        y[mask]
    )


def to_uniform_grid(x, *y):
    """Interpolate signals defined on arbitrary grid to uniform

    Uniform grid is inferred from x to have the same median step.

    Args:
        x  (np.ndarray | pd.Series): original time stamps in numeric or timedelta64(ns)
        *y (np.ndarray | pd.Series): arbitrary number of time series

    OR  x  (pd.DataFrame): first column is treated as timestamps, others — as time series

    Returns:
        x_grid, y_grid (np.ndarray): same signature as input, but interpoalated on grid
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
    fs_hz = 1 / (timestamps[1]-timestamps[0])
    nyq_hz = 0.5 * fs_hz
    wp = cutoff_hz / nyq_hz # lower edge of the passband
    k = 3 # more or less arbitrary, >=1
    ws = wp / k
    gpass = 1
    gstop = slope_db_oct * k / 2 # /2 is purely empiric. don't judge.
    N, Wn = signal.buttord(wp, ws, gpass, gstop)
    # print('butterworth\'s filter N =', N)
    return signal.butter(N, Wn, btype='highpass', output=filter_out)


def filter_array(t, x):
    sos = create_butterworth_hpf(FILTER_CONF['cutoff'], FILTER_CONF['slope'], t)
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


if __name__ == "__main__":
    print(intervals_from_mask([0, 0, 0, 1, 1, 1, 0, 0, 1, 0, 0, 1, 1]))
    print(smooth_intervals([(3, 6), (8, 9), (11, 13)]))