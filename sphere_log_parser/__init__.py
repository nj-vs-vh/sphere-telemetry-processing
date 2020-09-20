"""Sphere log parser, refactored to package. Parsed values, defaults
and parsing methods are stored together in data parser classes, derived
from collections.namedtuple with additional static methods

Standard package use:
>>> import sphere_log_parser as slp
>>> df = slp.read_log_to_dataframe('my_log_file.txt',
        parsing_config=slp.GROUND_DATA_CONFIG,
        logging=True)
>>> df.head()

To add new data fields for parsing, see data_parsers.py, _DataParserFactory function

To create new config from existing data parsers, see parsing_configs.py. Configs
are just lists of data parser classes.
To see all available data parsers:
>>> slp.print_config(ALL_FIELDS_CONFIG)
"""

from .parsing_configs import *  # __all__ property defines wildcard import for all-caps variables
from .main import read_log_to_dataframe, yield_log_records_as_dicts