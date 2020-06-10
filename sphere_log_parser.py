import re
import datetime
import pandas as pd
import numpy as np

import time


_TEST_LOG_FILENAME = '.\\data\\log_ice\\log.txt'


def extract_log_record(f, record_break_line = '-'*5):
	"""
		Yield lines from log record until record break
		is met
	"""
	rec = []
	for line in f:
		# check for eof
		if not line or (line.find(record_break_line) != -1):
			return iter(rec)
		rec.append(line[:-1]) # strip newline characters


def parse_log_record(rec):
	"""
		Parse log record (list of strings) to data dict
	"""

	def get_next_nonempty_line(lineit):
		line = next(lineit, None)
		while not line:
			line = next(lineit)
		return line

	def parse_current_line(value_present, data_name_dflt, line_parser):
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
		line_data = {name : default for name, default in data_name_dflt}
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
	data_name_default = [('datetime', np.datetime64('NaT'))]
	parse_current_line(
		lambda s: True, # condition for parsed values
		data_name_default, # names of parsed values
		lambda l: { # line parser
			data_name_default[0][0] : np.datetime64( datetime.datetime.strptime(l, '%a %b %d %H:%M:%S %Y'))\
					}
	)

	# parse GPS data from GPGGA format
	# ex: '$GPGGA 063059 5147.8142 N 10423.3275 E 1 09 0.9 447.3 M -37.2 M  *64'
	data_name_default = [('N, lat', np.NaN),
				  ('E, lon', np.NaN),
				  ('H, m', np.NaN),
				  ('GPS time', np.timedelta64('NaT', 's'))]
	def GPGGA_line_parser(line):
		ld = {}
		GPGGA = line.split()
		ld[data_name_default[0][0]] = float(GPGGA[2])
		ld[data_name_default[1][0]] = float(GPGGA[4])
		ld[data_name_default[2][0]] = float(GPGGA[9])
		t = GPGGA[1]
		# convert to seconds since day start and store in np datetime
		t = 3600*int(t[:2]) + 60*int(t[2:4]) + int(t[4:])
		ld[data_name_default[3][0]] = np.timedelta64(t, 's')
		return ld
	
	parse_current_line(
		lambda l: l.find('$GPGGA') != -1,
		data_name_default,
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
		return {data_name_default[0][0] : p, data_name_default[1][0] : T}
	
	data_name_default = [('P0, hPa', np.NaN),
				  ('T0, C', np.NaN)]
	parse_current_line(
		lambda l: l.find('0 Bar:') != -1,
		data_name_default,
		press_temp_line_parser
	)

	data_name_default = [('P1, hPa', np.NaN),
				  ('T1, C', np.NaN)]
	parse_current_line(
		lambda l: l.find('1 Bar:') != -1,
		data_name_default,
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
	n_rec = int((line_count(filename)-1) / 15) + 3
	print(f'~{n_rec} records found in log file')

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
				dtype = np.dtype(row_data[key])
			except TypeError:
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
		rec = extract_log_record(f, record_break_line)
		while rec and i_rec < n_rec:
			# parse current record to row
			row_data = parse_log_record(rec)
			if row_data is None:
				break
			# write row to data dict
			for key in data.keys():
				data[key][i_rec] = row_data[key]
			i_rec += 1
			# at last, prepare next record iterator
			rec = extract_log_record(f, record_break_line)
		
	print('parsing done!')

	# cut unused rows from each array
	for key in data.keys():
		data[key] = data[key][:i_rec]
	# convert to DataFrame
	return pd.DataFrame(data=data)

if __name__ == '__main__':
	df = read_log_to_dataframe(_TEST_LOG_FILENAME)
	df.to_csv('.\\data\\log_parsed.tsv', sep='\t')