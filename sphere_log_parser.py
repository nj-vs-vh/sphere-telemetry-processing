import re
import datetime
import pandas as pd
import numpy as np

from collections import OrderedDict

import time


_TEST_LOG_FILENAME = '.\\data\\log_ice\\log.txt'
#_TEST_LOG_FILENAME = '.\\data\\log_xx_2010.03.18_to_2012.03.12.txt'

# lines per log record; if None, infer from first record
# might be useful for manual override, if first record is long
# and others are shorter
RECORD_LENGTH_OVERRIDE = None

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


def parse_log_record(rec):
	"""
		Parse log record (list of strings) to data dict
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
		nonlocal data
		nonlocal rec
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
		# merge line data to final dict
		data = {**data, **line_data}

	data  = {} # initialize parsing result dictionary
	if rec is None:
		return None
	
	# parse date/time data
	# ex: 'Wed Mar 14 14:31:50 2012'
	line_data_default = {'datetime' : np.datetime64('NaT')}
	scan_record_for_line(
		lambda s: True, # try every line until one is parsed to datetime
		line_data_default,
		lambda l: {
			list(line_data_default)[0] : np.datetime64( datetime.datetime.strptime(l, '%a %b %d %H:%M:%S %Y'))\
		}
	)

	# parse GPS data from GPGGA format
	# ex: '$GPGGA 063059 5147.8142 N 10423.3275 E 1 09 0.9 447.3 M -37.2 M  *64'
	line_data_default = {'N, lat' : np.NaN,
				  	 	 'E, lon' : np.NaN,
					 	 'H, m' : np.NaN,
						 'GPS stamp' : -1
						 #'GPS time' : np.timedelta64('NaT', 's'),
						}
	def GPGGA_line_parser(line):
		ld = {}
		GPGGA = line.split()
		names = list(line_data_default)
		ld[names[0]] = float(GPGGA[2])
		ld[names[1]] = float(GPGGA[4])
		ld[names[2]] = float(GPGGA[9])
		t_str = GPGGA[1]
		ld[names[3]] = int(t_str)
		# convert to seconds since day start and store in np timedelta64
		# t = 3600*int(t_str[:2]) + 60*int(t_str[2:4]) + int(t_str[4:])
		# ld[names[4]] = np.timedelta64(t, 's')
		return ld
	
	scan_record_for_line(
		lambda l: l.find('$GPGGA') != -1,
		line_data_default,
		GPGGA_line_parser
	)

	# parse pressure and temperature data
	# ex: '0 Bar:  T[ 30983 ] = 30.2 C  P[ 29401 ] = 93.64 kPa (9545.1 mm w)'
	def press_temp_line_parser(l):
		# try to find string in parentesis with mm w units
		p_mmwater = re.findall(r'\(.*.mm.w\)', l)
		if len(p_mmwater)>0: # if found, read mm w pressure
			# conversion constant
			mmwater_per_hpa = 0.10197162129779 * 100
			# find all digits and decimal point, then glue them,
			# convert to float, convert to hPa
			p = float(''.join(re.findall(r'[\d.]', p_mmwater[0])))/mmwater_per_hpa
		else:
			# else try to find pressure in hpa
			p = float(re.findall(r'=.{7,10}hpa', l)[0].strip(' =hpa'))
		# temperature is between '=' and 'C'
		T = float(re.findall(r'= .* C', l)[0].strip('=C '))
		codes = re.findall(r'\[ .{3,9} \]', l)
		T_code = int(codes[0].strip(' []'))
		P_code = int(codes[1].strip(' []'))
		line_data_names = list(line_data_default)
		return {line_data_names[0] : p,
				line_data_names[1] : T,
				line_data_names[2] : T_code,
				line_data_names[3] : P_code}
	
	press_temp_i = 0
	line_data_default = {'P{}, hPa'.format(press_temp_i) : np.NaN,
						 'T{}, C'.format(press_temp_i) : np.NaN,
						 'T{}_code'.format(press_temp_i) : -1, # NaN can't handle integer values
						 'P{}_code'.format(press_temp_i) : -1}
	scan_record_for_line(
		lambda l: l.find('0 Bar:') != -1,
		line_data_default,
		press_temp_line_parser
	)

	press_temp_i = 1
	line_data_default = {'P{}, hPa'.format(press_temp_i) : np.NaN,
						 'T{}, C'.format(press_temp_i) : np.NaN,
						 'T{}_code'.format(press_temp_i) : -1, # NaN can't handle integer values
						 'P{}_code'.format(press_temp_i) : -1}
	scan_record_for_line(
		lambda l: l.find('1 Bar:') != -1,
		line_data_default,
		press_temp_line_parser
	)

	return data


def line_count(fnm):
	"""
		Preliminary file scan to determine line count
	"""
	with open(fnm, 'r') as f:
		for i, _ in enumerate(f): pass
	return i + 1


def read_log_to_dataframe(filename, record_break_line = '-'*5):
	"""
		Parse all records from log file specified with 'filename'
		and return as pandas.DataFrame

		Optional: custom record break line (checked for inclusion in line)
	"""
	# count log records to preallocate memory
	print(f'parsing {filename} for telemetry data')
	print(f'scanning for line count...')
	n_lines = line_count(filename)
	print(f'{n_lines} lines found in log')

	# scan file record-by-record and parse to dict,
	# then create series from dict and add as a row
	# to final DataFrame
	with open(filename, 'r', buffering=2**24) as f:
		# read first record from log
		rec = extract_log_record(f, record_break_line)
		row_data = parse_log_record(rec)

		# count lines in record to estimate records per log
		if RECORD_LENGTH_OVERRIDE is None:
			# +1 is for record break line (not included in rec)
			# -1 is to slightly underestimate record length, i.e.
			# overestimate number of records and preallocate
			# bigger arrays
			rec_len = len(rec) + 1 - 1
			print(f'record length is estimated from 1st record to be {rec_len}')
		else:
			rec_len = RECORD_LENGTH_OVERRIDE
			print(f'record length is manually set in RECORD_LENGTH_OVERRIDE constant to be {rec_len}')
		n_rec = (n_lines // rec_len) + 1
		print(f'number of records in log is estimated as {n_rec}')

		# preallocate numpy.ndarray based on first row
		data = dict.fromkeys(row_data.keys())
		print('extracting values:')
		for key in data.keys():
			try: # if data is already in numpy data type
				dtype = row_data[key].dtype
			except AttributeError:
				# else cast from python type to numpy dtype
				dtype = np.dtype(type(row_data[key]))
			data[key] = np.ndarray(shape = (n_rec), dtype = dtype)
			data[key][0] = row_data[key]
			print(f'{key}, type {dtype}')
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
			row_data = parse_log_record(rec)
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
		
	print(f'done! {i_rec} records parsed')

	# cut unused rows from each array
	for key in data.keys():
		data[key] = data[key][:i_rec]
	# convert to DataFrame
	return pd.DataFrame(data=data)

if __name__ == '__main__':
	df = read_log_to_dataframe(_TEST_LOG_FILENAME)
	df.to_csv('.\\data\\log_parsed.tsv', 
			  sep='\t',
			  date_format='%x %X')