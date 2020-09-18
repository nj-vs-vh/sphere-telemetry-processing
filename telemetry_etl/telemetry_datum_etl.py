"""Module for parsing telemetry data from logs and storing in local MongoDB"""

from pymongo import MongoClient
import numpy as np
import pandas as pd

from tqdm import tqdm

from datum_querying import read_datum_for_year
 
# assuming Mongo is running as mongod process/service and listening on localhost port 27017
# for installation see https://docs.mongodb.com/manual/administration/install-community/
# for restoring data from dump see README.md
client = MongoClient()
datum_telemetry_collection = client.sphere_telemetry.from_datum

columns_to_drop = {
    'Gqi',
    'Ggs',
    'H-455',
    'run',
    'dN',
    'dE',
    'Unnamed: 0',
}

column_name_unification = {
    'datetime': 'utc_dt',
    'N': 'N_lat',
    'E': 'E_lon',
    'H': 'H_m',
    'Gsn': 'Nsat',
    'Ghdp': 'HDOP',
    'P_hpa0': 'P0_hPa',
    'T0,C': 'T0_C',
    'P_hpa1': 'P1_hPa',
    'T1,C': 'T1_C',
    'U15,V': 'U15',
    'U5,V': 'U5',       
    'Uac,V': 'Uac',
    'I,A': 'I',
    'Tpow,C': 'Tp_C',
    'Tmos,C': 'Tm_C',
    'ClinTh': 'Clin_theta',
    'Bot,C': 'Tbot_C',
    'Top,C': 'Ttop_C',
}


for year in range(2009, 2014):
    datum = read_datum_for_year(year)

    datum.drop(columns=columns_to_drop, inplace=True)
    datum.rename(columns=column_name_unification, inplace=True)

    print(f'loading datum for the year {year}...')
    for _, record_series in tqdm(datum.iterrows()):
        record_series.dropna(inplace=True)
        record = record_series.to_dict()
        try:
            datum_telemetry_collection.update_one(
                filter={"utc_dt": {"$eq": record['utc_dt']}},
                update={
                    "$setOnInsert": {
                        key: val for key, val in record.items() if val != -1
                    }
                },
                upsert=True
            )
        except ValueError as e:
            print(e)