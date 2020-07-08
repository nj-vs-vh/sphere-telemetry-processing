import pandas as pd
import matplotlib.pyplot as plt

from visualizations import animate_filtered_scatter, plot_filtering, plot_correlation
from datum_processing import continuous_chunks
from config import DATA_DIR, DATUM_FILES


if __name__ == "__main__":
    df = pd.read_csv(DATA_DIR + DATUM_FILES[0])
    for ch in continuous_chunks(df):
        plot_filtering(ch)
        # plot_correlation(ch)