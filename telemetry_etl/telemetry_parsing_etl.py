"""Module for parsing telemetry data from logs and storing in local MongoDB"""

from pymongo import MongoClient
import numpy as np
import pandas as pd

from tqdm import tqdm

from sphere_log_parser import yield_log_as_dicts

# assuming Mongo is running as mongod process/service and listening on localhost port 27017
# for installation see https://docs.mongodb.com/manual/administration/install-community/
# for restoring data from dump see README.md
client = MongoClient()
onboard_telemetry_collection = client.sphere_telemetry.onboard


log_filenames = [
    'log_onboard_2010.03.18_to_2012.03.12.txt',
    'log_onboard_2012.03.12_to_2012.03.14.txt',
    'log_onboard_2012.03.19_to_2012.03.27.txt',
    'log_onboard_2013.02.16_to_2013.03.16.txt'
]

log_paths = [f'data\\logs\\{fnm}' for fnm in log_filenames]


for log_path in log_paths:
    print(f'loading {log_path}...')
    for record in tqdm(yield_log_as_dicts(log_path)):
        try:
            dt = pd.to_datetime(record.pop('datetime'))
        except KeyError:
            continue

        record['local_dt'] = dt

        try:
            onboard_telemetry_collection.update_one(
                filter={"local_dt": {"$eq": dt}},
                update={"$setOnInsert": {key: val for key, val in record.items() if val not in {np.NaN, -1}}},
                upsert=True
            )
        except ValueError:
            continue