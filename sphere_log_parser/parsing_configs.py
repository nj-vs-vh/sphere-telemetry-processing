"""Parsing configs are lists of data parser objects, which are passed
to parse_log_record func in main.py. ALL_FIELDS is created automatically
and contains all available data parsers. Others may be added manually,
see GROUND_DATA_FIELDS for example (stick to ALL_CAPS naming convention).

Also, function to print parsing config is provided"""

import sphere_log_parser.data_parsers as dp
from collections import namedtuple

# automatically collect all non-private classes from data_parsers.py
ALL_FIELDS_CONFIG = []
for k, v in dp.__dict__.items():
    if (isinstance(v, type)
        and not (k[0] is '_')):
        ALL_FIELDS_CONFIG.append(v)

# manually created presets
GROUND_DATA_CONFIG = [
    dp.Datetime,
    dp.GPS_basic,
    dp.P_T_0,
    dp.P_T_1,
    dp.P_T_codes_0,
    dp.P_T_codes_1,
    dp.Inclin,
]

INCLINOMETER_INIT_CONFIG = [
    dp.Datetime,
    dp.GPS_basic,
    dp.Inclin,
]

# YOUR_CONFIG = [
#     dp.Datetime,
#     ...
# ]

# create __all__ list with all not-hidden all-cpas names
__all__ = []
for var in dict(globals()).keys():
    if var[0] is not '_' and var.isupper():
        __all__.append(var)

# pretty-printing func
def print_config(cnf, verbose=True):
    print('parsing config:')
    for dp in cnf:
        if verbose:
            print(f'\'{dp.__name__}\' data parser: \t', end='')
            print(*dp._fields, sep=' | ')
        else:
            print(dp.__name__)

__all__.append('print_config')


def merge_config_to_dict(cnf):
    merged_dict = dict()
    for dp in cnf:
        # instantiate data parser with no args to get defaults
        # and store them in dictionary
        merged_dict = {**merged_dict, **dp()._asdict()}
    return merged_dict

if __name__ == "__main__":
    print_config(ALL_FIELDS_CONFIG)
