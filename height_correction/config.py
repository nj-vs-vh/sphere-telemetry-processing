DATA_DIR = ".\\data\\datum_tables\\"
DATUM_FILES = [
    "datum_2013_sec.csv",
    # 'datum_2012_sec.csv'
]

INDICATOR_THRESHOLD = 5  # m

INTERP_TYPE = "linear"

FILTER_CONF = {
    "cutoff": 4.16e-4,  # Hz, must be less than 5e-2 (Nyquist freq for signal with 10 s sampling)
    "slope": 12,  # db per octave
}

CORRECTION_CONF = {
    # 'correlation_window': 1200, # sec
    "ma_window": 300,  # sec
    "fitting_window": 120,  # sec
}

PSEUDOHEIGH_CONF = {
    "H0": 456,  # m, Baikal surface
    "p0": 1000,  # hPa, around average in march
    "a": 8400,  # m, R*T/Mg, approximate
}

TEMP_DIFF_SAMPLE_SAVING = False
TEMP_DIFF_SAMPLE_FILE = "temp.txt"
