"""Classes representing different data fields, parsed from SPHERE log files"""

import numpy as np
import re
import datetime

from collections import namedtuple


def _DataParserFactory(base_namedtuple: namedtuple,
                      parsemethod: callable,
                      foundmethod: callable = lambda _: True):
    """Main function for adding new classes of data parsers.
    Requires <base_namedtuple> with desired fields and required
    default values for all of them; <parsemethod> that takes string
    and returns object of <base_namedtuple> class; optional
    <foundmethod>, that takes string and returns bool value
    if string looks parsable (increases performance)
    """
    try:
        base_namedtuple()
    except TypeError:
        raise TypeError('Invalid data parser definition: '+
                        'base namedtuple must specify default values ' +
                        'for all fields')
    base_namedtuple.parse = staticmethod(parsemethod)
    base_namedtuple.found = staticmethod(foundmethod)
    return base_namedtuple


Datetime = _DataParserFactory(
    namedtuple('Datetime',
        field_names=['datetime'],
        defaults=[np.datetime64('NaT')]
    ),
    lambda line: Datetime(
        np.datetime64(
            datetime.datetime.strptime(line, '%a %b %d %H:%M:%S %Y')
        )
    ),
    lambda line: True
)


def gps_basic_parser(line):
    # see http://aprs.gids.nl/nmea/#gga
    gpgga = line.split()
    return GPS_basic(
        N_lat=float(gpgga[2]),
        E_lon=float(gpgga[4]),
        H_m=float(gpgga[9]),
        GPS_stamp=int(gpgga[1])
    )

GPS_basic = _DataParserFactory(
    namedtuple('GPS_basic',
        field_names=['N_lat', 'E_lon', 'H_m', 'GPS_stamp'],
        defaults=[np.NaN, np.NaN, np.NaN, -1]),
    gps_basic_parser,
    lambda line: '$GPGGA' in line
)


def gps_adv_parser(line):
    # see http://aprs.gids.nl/nmea/#gga
    gpgga = line.split()
    return GPS_advanced(
        Nsat=int(gpgga[7]),
        HDOP=float(gpgga[8])
    )

GPS_advanced = _DataParserFactory(
    namedtuple('GPS_advanced',
        field_names=['Nsat', 'HDOP'],
        defaults=[-1, np.NaN]
    ),
    gps_adv_parser,
    GPS_basic.found
)


def _P_T_parser_factory(i):
    P_T_class = namedtuple(f'P_T_{i}',
        field_names=[f'P{i}_hPa', f'T{i}_C'],
        defaults=[np.NaN, np.NaN])
    def parse(line):
        try:
            P = _parse_value_from_between(line, '=', 'hPa', float)
        except ValueError:
            P = 10*_parse_value_from_between(line, '=', 'kPa', float)
        try:
            T = _parse_value_from_between(line, '=', 'C', float)
        except Exception:
            return P_T_class(P)  # allow for strings with no T data
        return P_T_class(P,T)
    return _DataParserFactory(
        P_T_class,
        parse,
        lambda line: f'{i} Bar:' in line
    )

P_T_0 = _P_T_parser_factory(0)
P_T_1 = _P_T_parser_factory(1)


def _P_T_codes_parser_factory(i):
    P_T_codes_class = namedtuple(f'P_T_{i}_codes',
        field_names=[f'P{i}_code', f'T{i}_code'],
        defaults=[-1, -1])
    def parse(line):
        return P_T_codes_class._make([
            _parse_value_from_between(line, 'P[', ']', int), # P code
            _parse_value_from_between(line, 'T[', ']', int)  # T code
        ])
    return _DataParserFactory(
        P_T_codes_class,
        parse,
        lambda line: f'{i} Bar:' in line
    )

P_T_codes_0 = _P_T_codes_parser_factory(0)
P_T_codes_1 = _P_T_codes_parser_factory(1)


def inclin_parser(line):
    try:
        clin = line.split()
        return Inclin(
            Clin1=float(clin[0]),
            Clin2=float(clin[1])
        )
    except Exception:
        clin = _parse_value_from_between(line, 'Clin:', 'gr', str).split()
        return Inclin(
            Clin1=float(clin[0]),
            Clin2=float(clin[1])
        )

Inclin = _DataParserFactory(
    namedtuple('Inclinometer',
        field_names=['Clin1', 'Clin2'],
        defaults=[np.NaN, np.NaN]
    ),
    inclin_parser,
    lambda line: ('grad' in line) or ('Clin' in line)
)


Inclin_theta = _DataParserFactory(
    namedtuple('Inclin_theta',
        field_names=['Clin_theta'],
        defaults=[np.NaN]
    ),
    lambda line: Inclin_theta(
        _parse_value_from_between(line, 'Th:', 'gr', float)
    ),
    lambda line: 'Clin' in line
)


def power_parser(line):
    try:
        I_code = _parse_value_from_between(line, '=', 'kod', int)
    except Exception:
        I_code = -1
    return Power(
        U15=_parse_value_from_between(line, 'U15=', 'V', float),
        U5=_parse_value_from_between(line, 'U5=', 'V', float),
        Uac=_parse_value_from_between(line, 'Uac=', 'V', float),
        I=_parse_value_from_between(line, 'I=', 'A', float),
        I_code=I_code
    )

Power = _DataParserFactory(
    namedtuple('Power',
        field_names=['U15', 'U5', 'Uac', 'I', 'I_code'],
        defaults=[np.NaN, np.NaN, np.NaN, np.NaN, -1]
    ),
    power_parser,
    lambda line: 'Uac' in line
)


def _Temperature_factory(id):
    T_class = namedtuple(f'T{id}_C',
        field_names=[f'T{id}_C'],
        defaults=[np.NaN])
    return _DataParserFactory(
        T_class,
        lambda line: T_class(
            _parse_value_from_between(line, '=', 'oC', float)
        ),
        lambda line: f'T{id}' in line
    )

Tp = _Temperature_factory('p')
Tm = _Temperature_factory('m')


Compass = _DataParserFactory(
    namedtuple('Compass',
        field_names=['compass'],
        defaults=[np.NaN]
    ),
    lambda line: Compass(
        _parse_value_from_between(line, 'Compass:', 'gr', float))
    ,
    lambda line: 'Compass' in line
)


Led = _DataParserFactory(
    namedtuple('Led',
        field_names=['Led_ch0', 'Led_ch1', 'Led_ch2', 'Led_ch3'],
        defaults=[-1, -1, -1, -1]
    ),
    lambda line: Led(
        Led_ch0=_parse_value_from_between(line, 'CH0[', ']', int),
        Led_ch1=_parse_value_from_between(line, 'CH1[', ']', int),
        Led_ch2=_parse_value_from_between(line, 'CH2[', ']', int),
        Led_ch3=_parse_value_from_between(line, 'CH3[', ']', int),
    ),
    lambda line: 'LED:' in line
)


# Helper functions


def _parse_value_from_between(line,
                             lbnd, rbnd,
                             target_type):
    """
    Extract value between 'lbnd' and 'rbnd' from 'line' and
    cast it to 'type'. If there are several values, return first.
    No type check is performed, outside try-except expected. Trailing
    spaces are stripped.
    """
    revline = line[::-1]
    linelength = len(line)
    lpos, rpos = 0, len(line)
    lre = re.compile(re.escape(lbnd))  # search for first occurence of lbnd ...
    rre = re.compile(re.escape(rbnd[::-1]))  # ... and !last! occurence of rbnd
    while True:
        lsearch = lre.search(line, lpos, rpos)
        # move left from the first met lbnd
        rsearch = rre.search(revline, linelength-rpos, linelength-lpos)
        if lsearch is not None:
            # move right from the first met lbnd
            lpos = lsearch.span()[1]
        if rsearch is not None:
            # move left from the last met rbnd
            rpos = linelength-rsearch.span()[1]
        if lsearch is rsearch:  # i.e. both are None!
            return target_type(line[lpos:rpos].strip())