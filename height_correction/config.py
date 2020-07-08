DATA_DIR = ".\\data\\datum_tables\\"
DATUM_FILES = [
    'datum_2013_sec.csv'
]

INTERP_TYPE = 'linear'

FILTER_CONF = {
    'cutoff': 2e-4, # Hz, must be less than 5e-2 (Nyquist freq for signal with 10 s sampling)
    'slope': 12 # db per octave
}

CORRELATION_CONF = {
    'window': 1200 # sec
}