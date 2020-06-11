import re
import datetime
import pandas as pd
import numpy as np

from collections import OrderedDict

import time


_TEST_LOG_FILENAME = '.\\data\\log_ice\\log.txt'
EXPECTED_RECORD_LENGTH = 14 # lines per log record


def extract_log_record(f, record_break_line = '-'*5):
	"""
		Return iterator for lines in record
	"""
	rec = []
	for line in f:
		# check for eof
		if (line.find(record_break_line) != -1):
			return iter(rec)
		rec.append(line[:-1]) # strip newline characters
	return None


def parse_log_record(rec):
	"""
		Parse log record (list of strings) to data dict
	"""

	def get_next_nonempty_line(lineit):
		line = next(lineit, None)
		while not line:
			line = next(lineit)
		return line

	def parse_current_line(value_present, line_data_default, line_parser):
		"""
			Wrapper for line_parser that handles default values (if current line
			doesn't satisfy condition value_preset or if exception is raised while
			parsing), switches to the next line only if needed, and merges data
			from line to resulting dictionary
		"""
		nonlocal data
		nonlocal line
		# initialize dictionary of data in line with
		# default values
		line_data = line_data_default
		if value_present(line):
			try:
				# try to use line parser to get dict
				# with actual data
				line_data = line_parser(line)
			except: pass
			# if data is present switch to the next line
			line = get_next_nonempty_line(rec)
		# merge line data to final dict
		data = {**data, **line_data}

	data  = {} # initialize parsing result dictionary
	line = get_next_nonempty_line(rec)
	if line is None: # check for empty record
		return None
	
	# parse date/time data, ex: 'Wed Mar 14 14:31:50 2012'
	line_data_default = {'datetime' : np.datetime64('NaT')}
	parse_current_line(
		lambda s: True, # condition for parsed values
		line_data_default, # names of parsed values
		lambda l: { # line parser
			list(line_data_default)[0] : np.datetime64( datetime.datetime.strptime(l, '%a %b %d %H:%M:%S %Y'))\
		}
	)

	# parse GPS data from GPGGA format
	# ex: '$GPGGA 063059 5147.8142 N 10423.3275 E 1 09 0.9 447.3 M -37.2 M  *64'
	line_data_default = {'N, lat' : np.NaN,
				  	 	 'E, lon' : np.NaN,
					 	 'H, m' : np.NaN,
						 'GPS time' : np.timedelta64('NaT', 's'),
						 'GPS stamp' : np.unicode_('      ') # 6 spaces
						}
	def GPGGA_line_parser(line):
		ld = {}
		GPGGA = line.split()
		names = list(line_data_default)
		ld[names[0]] = float(GPGGA[2])
		ld[names[1]] = float(GPGGA[4])
		ld[names[2]] = float(GPGGA[9])
		t_str = GPGGA[1]
		# convert to seconds since day start and store in np datetime
		t = 3600*int(t_str[:2]) + 60*int(t_str[2:4]) + int(t_str[4:])
		ld[names[3]] = np.timedelta64(t, 's')
		ld[names[4]] = np.unicode_(t_str) # encoded for storage
		return ld
	
	parse_current_line(
		lambda l: l.find('$GPGGA') != -1,
		line_data_default,
		GPGGA_line_parser
	)

	# parse pressure and temperature data
	# ex: '0 Bar:  T[ 30983 ] = 30.2 C  P[ 29401 ] = 93.64 kPa (9545.1 mm w)'
	def press_temp_line_parser(l):
		# conversion constant
		mmwater_per_hpa = 0.10197162129779 * 100
		# match string in parentesis with mm w units
		mmwater_str = re.findall(r'\(.* mm w\)', l)
		# find all digits and decimal point, then glue them,
		# convert to float, convert to hPa
		p = float(''.join(re.findall(r'[\d.]', mmwater_str[0])))/mmwater_per_hpa
		# temperature is between '=' and 'C'
		T = float(re.findall(r'= .* C', l)[0].strip('=C '))
		line_data_names = list(line_data_default)
		return {line_data_names[0] : p, line_data_names[1] : T}
	
	line_data_default = {'P0, hPa' : np.NaN , 'T0, C' : np.NaN}
	parse_current_line(
		lambda l: l.find('0 Bar:') != -1,
		line_data_default,
		press_temp_line_parser
	)

	line_data_default = {'P1, hPa' : np.NaN , 'T1, C' : np.NaN}
	parse_current_line(
		lambda l: l.find('1 Bar:') != -1,
		line_data_default,
		press_temp_line_parser
	)
	
	# skip everything else
	while True:
		if next(rec, None) is None: break

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
	n_rec = ((n_lines) // EXPECTED_RECORD_LENGTH) + 1
	print(f'{n_lines} lines found in log, resulting in {n_rec} records ' + 
		  f'({EXPECTED_RECORD_LENGTH} lines per record expected)')

	# scan file record-by-record and parse to dict,
	# then create series from dict and add as a row
	# to final DataFrame
	with open(filename, 'r', buffering=2**24) as f:
		rec = extract_log_record(f, record_break_line)
		row_data = parse_log_record(rec)

		# preallocate numpy.ndarray based on first row
		data = dict.fromkeys(row_data.keys())
		for key in data.keys():
			try: # if data is already in numpy data type
				dtype = row_data[key].dtype
			except AttributeError:
				# else cast from python type to numpy dtype
				dtype = np.dtype(type(row_data[key]))
			data[key] = np.ndarray(shape = (n_rec), dtype = dtype)
			data[key][0] = row_data[key]

		# 'head' output
		#print('extracting data:')
		#for key in data.keys():
		#	print(f'\t{key} (e.g. {data[key][0]})')

		# pre-init the loop
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
				raise ValueError('Preallocated size for parsed log data is too small, ' + 
								 'check if EXPECTED_RECORD_LENGTH constant is right for ' +
								 'your log (if there are variable-length records, use the shortest)')
		
	print(f'parsing done! {i_rec} records parsed')

	# cut unused rows from each array
	for key in data.keys():
		data[key] = data[key][:i_rec]
	# convert to DataFrame
	return pd.DataFrame(data=data)

if __name__ == '__main__':
	df = read_log_to_dataframe(_TEST_LOG_FILENAME)
	df.to_csv('.\\data\\log_parsed.tsv', sep='\t')