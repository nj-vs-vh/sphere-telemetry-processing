import re
import datetime
import pandas as pd
import numpy as np
import codecs

from collections import OrderedDict

import time


#_TEST_LOG_FILENAME = '.\\data\\log_ice\\log.txt'
#_TEST_LOG_FILENAME = '.\\data\\2013_logs\\log_ground_2013.03.10_to_2013.03.16.txt'
_TEST_LOG_FILENAME = '.\\data\\logs\\log_utf_test.txt'

# lines per log record; if None, infer from first record
# might be useful for manual override, if first record is long
# and others are shorter
RECORD_LENGTH_OVERRIDE = None

LOGGING = True

# pre-defined parsing fields' sets
TIME_FIELDS = {'datetime', 'GPS_stamp'}
POSITION_FIELDS = {'N_lat', 'E_lon', 'H_m'}
GPS_ADVANCED_FIELDS = {'Nsat', 'HDOP'}
P_T_FIELDS = {'P0_hPa', 'T0_C', 'P0_code', 'P1_hPa', 'T1_C'}
P_T_CODES_FIELDS = {'P0_code', 'T0_code', 'P1_code', 'T1_code'}
INCLIN_FIELDS = {'Clin1', 'Clin2'}
POWER_FIELDS = {'U15', 'U5', 'Uac', 'I'}
TEMPERATURE_FIELDS = {'Tp_C', 'Tm_C'}

# composite fields
ALL_FIELDS = (TIME_FIELDS | POSITION_FIELDS |
			  GPS_ADVANCED_FIELDS | P_T_FIELDS |
			  P_T_CODES_FIELDS | INCLIN_FIELDS |
			  POWER_FIELDS | TEMPERATURE_FIELDS)
GROUND_DATA_FIELDS = (TIME_FIELDS | POSITION_FIELDS |
					  GPS_ADVANCED_FIELDS | P_T_FIELDS |
					  P_T_CODES_FIELDS)

##################### LINE-LEVEL FUNCTIONS #####################

def parse_value_from_between(line, lbnd, rbnd, target_type):
	"""
		Extract value between 'lbnd' and 'rbnd' from 'line' and 
		cast it to 'type'. If there are several values, return first.
		No type check is performed, outside try-except expected
	"""
	revline = line[::-1]
	linelength = len(line)
	lpos, rpos = 0, len(line)
	lre = re.compile(lbnd) # search for first occurence of lbnd ...
	rre = re.compile(rbnd[::-1]) # ... and !last! occurence of rbnd
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
		if lsearch is rsearch: # i.e. both are None!
			return target_type(line[lpos:rpos].strip())

##################### RECORD-LEVEL FUNCTIONS #####################

def extract_log_record(f, record_break_line = '-'*5):
	"""
		Return list of lines in record
	"""
	rec = []
	for line in f:
		# check for eof
		if (line.find(record_break_line) != -1):
			return(rec)
		rec.append(line[:-1]) # strip newline characters
	return None


def parse_log_record(rec, parsing_fields):
	"""
		Main parser: takes rec (list of strings) and returns dict
		with parsed data (fields are limited by parsing_fields set)
	"""

	def scan_record_for_line(condition, line_data_default, line_parser):
		"""
			Wrapper for line_parser function that scans for lines in record,
			finds one that satisfy condition (i.e. if word 'Bar' is in the line)
			and feeds it to line parser. Data pased from line are appended to
			global data dictionary; if no data is parsed, line_data_default is used
		"""
		# if true, scan all lines in record, even if value_present(line) is met
		GREEDY_LINE_SEARCH = True
		nonlocal parsing_fields
		nonlocal data
		nonlocal rec
		# check if current line's data keys are in target parsing fields
		# if not, leave
		if not parsing_fields & line_data_default.keys():
				return
		# initialize dictionary of data in line with default values
		line_data = line_data_default
		for line in rec:
			if condition(line):
				try:
					# try to use line parser to get dict
					# with actual data
					line_data = line_parser(line)
				except: pass
				finally:
					if not GREEDY_LINE_SEARCH:
						break
		# merge line data to final dict with resect to parsing_fields
		for key in line_data:
			if key in parsing_fields:
				data[key] = line_data[key]

	data  = {} # initialize parsing result dictionary
	if rec is None:
		return None
	
	# parse date/time data
	# ex: 'Wed Mar 14 14:31:50 2012'
	line_data_default = {'datetime' : np.datetime64('NaT')}
	scan_record_for_line(
		lambda line: True, # try every line until one is parsed to datetime
		line_data_default,
		lambda line: {
			list(line_data_default)[0] : np.datetime64( 
				datetime.datetime.strptime(line, '%a %b %d %H:%M:%S %Y')
			)
		}
	)

	# parse GPS data from GPGGA format
	# ex: '$GPGGA 063059 5147.8142 N 10423.3275 E 1 09 0.9 447.3 M -37.2 M  *64'
	line_data_default = {'N_lat' : np.NaN,
				  	 	 'E_lon' : np.NaN,
					 	 'H_m' : np.NaN,
						 'GPS_stamp' : -1,
						 'Nsat' : -1,
						 'HDOP' : np.NaN
						}
	def gpgga_line_parser(line):
		names = list(line_data_default)
		ld = dict.fromkeys(names)
		gpgga_line = line.split()
		# see http://aprs.gids.nl/nmea/#gga for legend
		ld[names[0]] = float(gpgga_line[2])
		ld[names[1]] = float(gpgga_line[4])
		ld[names[2]] = float(gpgga_line[9])
		ld[names[3]] = int(gpgga_line[1])
		ld[names[4]] = int(gpgga_line[7])
		ld[names[5]] = float(gpgga_line[8])
		return ld
	scan_record_for_line(
		lambda line: line.find('$GPGGA') != -1,
		line_data_default,
		gpgga_line_parser
	)

	# parse pressure and temperature data
	# ex: '0 Bar:  T[ 30983 ] = 30.2 C  P[ 29401 ] = 93.64 kPa (9545.1 mm w)'
	def generate_pressure_line_data_default(i):
		return {'P{}_hPa'.format(i) : np.NaN,
				'T{}_C'.format(i) : np.NaN,
				'P{}_code'.format(i) : -1,
				'T{}_code'.format(i) : -1 # NaN can't handle integer values
				}
	def press_temp_line_parser(line):
		names = list(line_data_default)
		ld = dict.fromkeys(names)
		try: # looking for pressure expressed in hPa
			ld[names[0]] = parse_value_from_between(line, '=', 'hPa', float)
		except ValueError: # if not found -- look for kPa
			ld[names[0]] = parse_value_from_between(line, '=', 'kPa', float) * 10
		try: # temperature in C
			ld[names[1]] = parse_value_from_between(line, '=', 'C', float)
		except: pass
		try: # P and T codes
			ld[names[2]] = parse_value_from_between(line, 'P\\[', ']\\', int)
			ld[names[3]] = parse_value_from_between(line, 'T\\[', ']\\', int)
		except: pass
		return ld
	line_data_default = generate_pressure_line_data_default(0)
	scan_record_for_line(
		lambda line: line.find('0 Bar:') != -1,
		line_data_default,
		press_temp_line_parser
	)
	line_data_default = generate_pressure_line_data_default(1)
	scan_record_for_line(
		lambda line: line.find('1 Bar:') != -1,
		line_data_default,
		press_temp_line_parser
	)

	# parse inclinometer data
	# ex: '0.0  0.0 grad'
	line_data_default = {'Clin1' : np.NaN, 'Clin2' : np.NaN}
	def inclinometer_parser(line):
		clin = line.split()
		names = list(line_data_default)
		return {names[0] : float(clin[0]),
				names[1] : float(clin[1])}
	scan_record_for_line(
		lambda line: line.find('grad') != -1,
		line_data_default,
		inclinometer_parser
	)

	# parse voltages/currents data
	# ex: 'U15=  15.01V  U5= 5.19V  Uac=  19.02V  I=   0.84A'
	line_data_default = {'U15' : np.NaN,
						 'U5' : np.NaN,
						 'Uac' : np.NaN,
						 'I' : np.NaN}
	def voltage_parser(line):
		names = list(line_data_default)
		ld = dict.fromkeys(names)
		ld[names[0]] = parse_value_from_between(line, 'U15=', 'V', float)
		ld[names[1]] = parse_value_from_between(line, 'U5=', 'V', float)
		ld[names[2]] = parse_value_from_between(line, 'Uac=', 'V', float)
		ld[names[3]] = parse_value_from_between(line, 'I=', 'A', float)
		return ld
	scan_record_for_line(
		lambda line: line.find('Uac') != -1,
		line_data_default,
		voltage_parser
	)

	line_data_default = {'Tp_C' : np.NaN}
	def temp_parser(line):
		return {
			list(line_data_default)[0] : parse_value_from_between(line, '=', 'oC', float)
		}
	scan_record_for_line(
		lambda line: line.find('Tp') != -1,
		line_data_default,
		temp_parser
	)

	line_data_default = {'Tm_C' : np.NaN}
	scan_record_for_line(
		lambda line: line.find('Tm') != -1,
		line_data_default,
		temp_parser
	)

	return data

##################### FILE-LEVEL FUNCTIONS #####################

def line_count(fnm):
	"""
		Preliminary file scan to determine line count
	"""
	with codecs.open(fnm, 'r', encoding='utf-8', errors='ignore') as f:
		for i, _ in enumerate(f): pass
	return i + 1


def read_log_to_dataframe(filename, record_break_line='-'*5, parsing_fields=ALL_FIELDS):
	"""
		Master function: parse all records from log file
		specified with 'filename' and return data as pandas.DataFrame

		Optional parameters:
			record_break_line -- str, record separator, checked for *inclusion* in line
			parsing_fields -- set, fields to be parsed. intended to use with one of the
			pre-defined sets at the start of the module
	"""
	global LOGGING
	# preliminary file sacn: count log records to preallocate memory
	if LOGGING:
		print(f'parsing {filename} for telemetry data')
		print(f'scanning for line count...')
	n_lines = line_count(filename)
	if LOGGING:
		print(f'{n_lines} lines found in log')

	# main file scan
	with codecs.open(filename, 'r', 
					 encoding='utf-8',
					 errors='ignore',
					 buffering=2**24) as f:
		# read first record from log
		rec = extract_log_record(f, record_break_line)
		row_data = parse_log_record(rec, parsing_fields)

		# count lines in record to estimate records per log
		if RECORD_LENGTH_OVERRIDE is None:
			# +1 is for record break line (not included in rec)
			# -1 is to slightly underestimate record length, i.e.
			# overestimate number of records and preallocate
			# bigger arrays
			rec_len = len(rec) + 1 - 1
			if LOGGING:
				print(f'record length is estimated from 1st record to be {rec_len}')
		else:
			rec_len = RECORD_LENGTH_OVERRIDE
			if LOGGING:
				print(f'record length is manually set in RECORD_LENGTH_OVERRIDE constant to be {rec_len}')
		n_rec = (n_lines // rec_len) + 1
		if LOGGING:
			print(f'number of records in log is estimated as {n_rec}')

		# preallocate numpy.ndarray based on first row
		data = dict.fromkeys(row_data.keys())
		if LOGGING:
			print('extracting values:')
		for key in data.keys():
			try: # if data is already in numpy data type
				dtype = row_data[key].dtype
			except AttributeError:
				# else cast from python type to numpy dtype
				dtype = np.dtype(type(row_data[key]))
			data[key] = np.ndarray(shape = (n_rec), dtype = dtype)
			data[key][0] = row_data[key]
			if LOGGING:
				print(f'\t{key}, type {dtype}')
		if LOGGING:
			print('...')

		# main loop
		i_rec = 1
		while True:
			# create current record iterator
			rec = extract_log_record(f, record_break_line)
			# if no record break is hit, rec is None
			# exit the loop
			if rec is None:
				break
			# parse current record to row
			row_data = parse_log_record(rec, parsing_fields)
			# write row to data dict
			for key in data.keys():
				data[key][i_rec] = row_data[key]
			i_rec += 1
			if i_rec >= n_rec:
				if RECORD_LENGTH_OVERRIDE is None:
					msg = ('Record length inferred from data is too big, ' + 
						   'try manually setting RECORD_LENGTH_OVERRIDE in source code ' + 
						   'to the size (in # of lines) of the shortest record in your log')
				else:
					msg = ('Overriden record length RECORD_LENGTH_OVERRIDE=' + 
						   '{} is too big, '.format(RECORD_LENGTH_OVERRIDE) + 
						   'try lesser value or None to infer record length from log')
				raise ValueError(msg)
	
	if LOGGING:
		print(f'done! {i_rec} records parsed')

	# cut unused rows from arrays
	for key in data.keys():
		data[key] = data[key][:i_rec]
	# convert to DataFrame
	return pd.DataFrame(data=data)

if __name__ == '__main__':
	df = read_log_to_dataframe(_TEST_LOG_FILENAME, parsing_fields=GROUND_DATA_FIELDS)
	df.to_csv('.\\data\\log_parsed.tsv', 
			  sep='\t',
			  date_format='%x %X')