import codecs
import pandas as pd
import numpy as np

# module with configs -- lists of data parser objects
from . import parsing_configs


_TEST_LOG_FILENAME = 'data\\logs\\log_onboard_example.txt'

# lines per log record; if None, infer from first record
# might be useful for manual override, if first record is long
# and others are shorter
RECORD_LENGTH_OVERRIDE = None


def extract_log_record(f, record_break_sequence='-'*5):
    """Return list of lines in current record as read
    from file object <f>. Record is closed and returned
    when a line, containing <record_break_sequence> is met.
    If no record is closed, return None"""
    rec = []
    for line in f:
        if (line.find(record_break_sequence) != -1):
            return(rec)
        rec.append(line[:-1])  # strip newline characters
    return None


def parse_log_record(rec, parsing_config):
    """Record-level parser: takes list of strings <rec>,
    scans and parses it with data_parser objects from 
    <parse_config> and returns named tuple with all
    fields from config
    """
    # check for empty record
    if rec is None:
        return None
    # create dict with default values
    rec_data = parsing_configs.merge_config_to_dict(parsing_config)
    for data_parser in parsing_config:
        # if true, scan all lines in record, even if parsable line is met
        GREEDY_LINE_SEARCH = True
        line_data = None
        for line in rec:
            if data_parser.found(line):
                try:
                    line_data = data_parser.parse(line)
                except Exception:
                    pass
                finally:
                    if not GREEDY_LINE_SEARCH:
                        break
        # merge parsed line data to overall rec data dict
        if line_data:
            for field in line_data._fields:
                rec_data[field] = getattr(line_data, field)
    return rec_data


def line_count(fnm):
    """Preliminary file scan to determine line count"""
    with codecs.open(fnm, 'r', encoding='utf-8', errors='ignore') as f:
        for i, _ in enumerate(f):
            pass
    return i + 1


def read_log_to_dataframe(filename,
                          parsing_config=parsing_configs.ALL_FIELDS_CONFIG,
                          logging=False,
                          record_break_seq='-'*5):
    """Main function: parse all records from log file
    specified with <filename> and return data as pandas.DataFrame

    Optional parameters:
    record_break_seq -- passed to extract_log_record func
    logging          -- bool flag for command line logging
    parsing_config   -- list of data parser objects,
                        see parsing_configs.py
    """
    # preliminary file sacn: count log records to preallocate memory
    if logging:
        print(f'parsing {filename} for telemetry data')
        print(f'scanning for line count...')
    n_lines = line_count(filename)
    if logging:
        print(f'{n_lines} lines found in log')

    # main file scan
    with codecs.open(filename, 'r',
                     encoding='utf-8',
                     errors='ignore',
                     buffering=2**24) as f:
        # read first record from log
        rec = extract_log_record(f, record_break_seq)
        row_data = parse_log_record(rec, parsing_config)

        # count lines in record to estimate records per log
        if RECORD_LENGTH_OVERRIDE is None:
            # +1 is for record break line (not included in rec)
            # -1 is to slightly underestimate record length, i.e.
            # overestimate number of records and preallocate
            # bigger arrays
            rec_len = len(rec) + 1 - 1
            if logging:
                print('record length is estimated from 1st record ' +
                      f'to be {rec_len}')
        else:
            rec_len = RECORD_LENGTH_OVERRIDE
            if logging:
                print(f'record length is manually set in ' +
                      'RECORD_LENGTH_OVERRIDE constant to be {rec_len}')
        n_rec = (n_lines // rec_len) + 1
        if logging:
            print(f'number of records in log is estimated as {n_rec}')

        # preallocate numpy.ndarray based on first row
        data = dict.fromkeys(row_data.keys())
        for key, val in row_data.items():
            try:  # if data is already in numpy data type
                dtype = val.dtype
            except AttributeError:
                # else cast from python type to numpy dtype
                dtype = np.dtype(type(row_data[key]))
            data[key] = np.ndarray(shape=(n_rec,), dtype=dtype)
            data[key][0] = row_data[key]
        if logging:
            parsing_configs.print_config(parsing_config)
            print('...')

        # main loop
        i_rec = 1  #0'th element is already set
        while True:
            rec = extract_log_record(f, record_break_seq)
            if rec is None:  # check for end of file
                break
            # parse current record to row
            row_data = parse_log_record(rec, parsing_config)
            for key in data.keys():  # write row to data dict
                data[key][i_rec] = row_data[key]
            i_rec += 1
            if i_rec >= n_rec:
                if RECORD_LENGTH_OVERRIDE is None:
                    msg = ('Record length inferred from data is too big, ' +
                           'try manually setting RECORD_LENGTH_OVERRIDE ' +
                           'in source code to the size (in # of lines) ' +
                           'of the shortest record in your log')
                else:
                    msg = ('Overriden record length RECORD_LENGTH_OVERRIDE=' +
                           f'{RECORD_LENGTH_OVERRIDE} is too big, ' +
                           'try lesser value or None to infer record ' +
                           'length from log')
                raise ValueError(msg)

    if logging:
        print(f'done! {i_rec} records parsed')

    # cut unused rows from arrays
    for key in data.keys():
        data[key] = data[key][:i_rec]
    # convert to DataFrame
    return pd.DataFrame(data=data)


def yield_log_records_as_dicts(filename, record_break_seq='-'*5):
    """Low-level parsing func, yields log records one-by-one as dicts

    Args:
        same as in read_log_to_dataframe
    """
    with codecs.open(
        filename, 'r', encoding='utf-8', errors='ignore', buffering=2**24
    ) as f:
        while True:
            rec = extract_log_record(f, record_break_seq)
            if rec is None:  # check for EOF
                break
            yield parse_log_record(rec, parsing_configs.ALL_FIELDS_CONFIG)


if __name__ == '__main__':
    from pprint import pprint

    telemetry_gen = yield_log_records_as_dicts(_TEST_LOG_FILENAME)
    for i, d in enumerate(telemetry_gen):
        pprint(d)
        if i > 10:
            break
