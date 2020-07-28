import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import correction

from visualizations import animate_filtered_scatter, plot_filtering, plot_correlation
from datum_processing import chunks_from_datum_list
from config import DATA_DIR, DATUM_FILES, PSEUDOHEIGH_CONF


def calculate_pseudo_height(df: pd.DataFrame):
    df.insert(
        len(df.columns),
        column = 'H_P',
        value = PSEUDOHEIGH_CONF['H0'] - PSEUDOHEIGH_CONF['a'] * np.log(df['P_hpa1'].to_numpy() / PSEUDOHEIGH_CONF['p0'])
    )


if __name__ == "__main__":
    res = pd.DataFrame()
    for ch in chunks_from_datum_list():
        calculate_pseudo_height(ch)
        plot_filtering(ch)
        correction.process_chunk(ch)
        res = res.append(ch)
    res.to_csv('datum_with_corrected_H.csv')