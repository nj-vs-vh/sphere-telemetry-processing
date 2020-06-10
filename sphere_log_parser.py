import re
import datetime
import pandas as pd
import numpy as np

import time


_TEST_LOG_FILENAME = '.\\log_ice\\log.txt'


def extract_log_record(f, record_break_line = '-'*23):
	"""
		Yield lines from log record until record break
		is met
	"""
	rec = []
	for line in f:
		# check for eof
		if not line or (line[:-1] == record_break_line):
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

	data  = {}
	line = get_next_nonempty_line(rec)
	if line is None:
		return None
	
	# parse date/time data, ex: 'Wed Mar 14 14:31:50 2012'
	data['datetime'] = np.datetime64( \
		datetime.datetime.strptime(line, \
			'%a %b %d %H:%M:%S %Y')
	)

	line = get_next_nonempty_line(rec)
	if line.find('$GPGGA') != -1:
		# if GPS data is present
		# parse it from GPGGA format
		# ex: '$GPGGA 063059 5147.8142 N 10423.3275 E 1 09 0.9 447.3 M -37.2 M  *64'
		GPGGA = line.split()
		data['N, lat'] = float(GPGGA[2])
		data['E, lon'] = float(GPGGA[4])
		data['H, m'] = float(GPGGA[9])
		line = get_next_nonempty_line(rec)
	else:
		# placeholder values
		data['N, lat'] = np.NaN
		data['E, lon'] = np.NaN
		data['H, m'] = np.NaN
	
	# parse pressure data
	# ex: '0 Bar:  T[ 30983 ] = 30.2 C  P[ 29401 ] = 93.64 kPa (9545.1 mm w)'
	mmwater_per_hpa = 0.10197162129779 * 100
	for i in range(2): # read two pressures at once
		mmwater_str = re.findall(r'\(.* mm w\)', line) # match string in parentesis with mm w units
		if mmwater_str:
			try:
				# find all digits and decimal point, then glue them
				# and convert to float
				p = float(''.join(re.findall(r'[\d.]', mmwater_str[0])))
				# units conversion
				p = p/mmwater_per_hpa
			except:
				p = np.NaN
		else:
			p = np.NaN
		data[f'P{i}, hPa'] = p # units conversion
		line = get_next_nonempty_line(rec)

	# parse pressure difference
	# ex: 'Pressure diff -2.23 kPa'
	if line.find('Pressure diff') != -1:
		pdiff = float(''.join(re.findall(r'[\d.-]', line)))
		line = get_next_nonempty_line(rec)
	else:
		pdiff = np.NaN
	data['dP, kPa'] = pdiff

	# parse inclinometer
	# ex: '0.0  0.0 grad'
	if line.find('grad') != -1:
		inclin = line.split()
		clin1 = inclin[0]
		clin2 = inclin[1]
	else:
		clin1, clin2 = np.NaN
	data['Clin1'] = clin1
	data['Clin2'] = clin2

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


def read_log_to_dataframe(fnm):
	"""
		Parse all records from log file and return as pandas DataFrame
	"""
	# count log records to preallocate memory
	print(f'parsing {fnm} for telemetry data')
	print(f'scanning for line count...')
	n_rec = int((line_count(fnm)-1) / 15) + 3
	print(f'~{n_rec} records found in log file')

	# scan file record-by-record and parse to dict,
	# then create series from dict and add as a row
	# to final DataFrame
	with open(fnm, 'r', buffering=2**24) as f:
		rec = extract_log_record(f)
		row_data = parse_log_record(rec)

		# preallocate numpy.ndarray based on first row
		data = dict.fromkeys(row_data.keys())
		for k in data.keys():
			try: # if data is already in numpy data type
				dtype = np.dtype(row_data[k])
			except TypeError:
				# cast from python type to numpy dtype
				dtype = np.dtype(type(row_data[k]))
			data[k] = np.ndarray(shape = (n_rec), dtype = dtype)

		# 'head' output
		print('extracting data:')
		for k in data.keys():
			print(f'\t{k} (e.g. {row_data[k]})')

		# pre-init the loop
		i_rec = 1
		rec = extract_log_record(f)
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
			rec = extract_log_record(f)
	
	# cut unused rows from each array
	for key in data.keys():
		data[key] = data[key][:i_rec]
	# convert to DataFrame
	return pd.DataFrame(data=data)

if __name__ == '__main__':
	df = read_log_to_dataframe(_TEST_LOG_FILENAME)
	df.to_csv('log_parsed.tsv', sep='\t')