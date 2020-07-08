import numpy as np
from scipy.stats import pearsonr


from config import CORRELATION_CONF


def moving_correlation(t, s1, s2):
    # sec -> t bin
    window = int(CORRELATION_CONF['window'] / (t[1]-t[0]))
    t_corr = t[window:]
    r_corr = np.zeros_like(t_corr, dtype=np.dtype('float64'))
    for i in range(window, len(t)):
        r_corr[i-window] = pearsonr(s1[i-window:i], s2[i-window:i])[0]
    return t_corr, r_corr