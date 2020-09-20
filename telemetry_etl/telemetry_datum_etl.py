"""Module for parsing telemetry data from logs and storing in local MongoDB"""

from pymongo import MongoClient
import numpy as np
import pandas as pd

from tqdm import tqdm

from datum_querying import datum_datetime
 
# assuming Mongo is running as mongod process/service and listening on localhost port 27017
# for installation see https://docs.mongodb.com/manual/administration/install-community/
# for restoring data from dump see README.md
client = MongoClient()
datum_telemetry_collection = client.sphere_telemetry.from_datum_tables

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


datum_filenames = [
    'datum_2009_sec.csv',
    'datum_2010_sec.csv',
    'datum_2011_sec.csv',
    'datum_2012_sec.csv',
    'datum_2013_sec.csv',
]

# store datum filenames with ids (used as foreign key)
datum_filenames_collection = client.sphere_telemetry.datum_filenames
for id_, datum_filename in enumerate(datum_filenames):
    datum_filenames_collection.update_one(
        filter={'id': id_},
        update={"$set": {'id': id_, 'filename': datum_filename}},
        upsert=True,
    )


for datum_filename in datum_filenames:
    datum_path = f'data\\datum_tables\\{datum_filename}'
    print(f'loading datum from {datum_path}...')
    datum_filename_id = datum_filenames_collection.find_one({'filename': datum_filename})['id']

    datum = pd.read_csv(datum_path)
    datum.insert(0, 'utc_dt', datum_datetime(datum))
    datum.set_index('utc_dt', inplace=True, drop=False)
    datum.index.name = 'local_dt_index'
    datum.drop(columns=['year', 'month', 'day', 'time'], inplace=True)

    datum.drop(columns=columns_to_drop, inplace=True)
    datum.rename(columns=column_name_unification, inplace=True)
    for _, record_series in tqdm(datum.iterrows()):
        record_series.dropna(inplace=True)
        record = record_series.to_dict()
        try:
            datum_telemetry_collection.update_one(
                filter={"utc_dt": {"$eq": record['utc_dt']}},
                update={
                    "$setOnInsert": {key: val for key, val in record.items() if val != -1},
                    "$set": {'source_id': datum_filename_id},
                },
                upsert=True
            )
        except ValueError as e:
            print(e)