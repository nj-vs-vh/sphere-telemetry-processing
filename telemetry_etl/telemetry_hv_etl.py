import pandas as pd
from pathlib import Path
from pymongo import MongoClient
from datetime import datetime
from tqdm import tqdm


client = MongoClient()


DATADIR = Path('/home/njvh/Documents/Science/sphere/telemetry/data/other sources')


coll = client.sphere_telemetry.hv_vip_pmt_mapping


df = pd.read_csv(DATADIR / 'hv_errors.csv')
df['vip'] = df['vip'] + 1
df.set_index('vip', inplace=True)
df = df.applymap(lambda s: s == 'ERR')
vip_errors = df.to_dict('dict')
# print(vip_errors['2013'][33])


df = pd.read_csv(DATADIR / 'hv_mapping.csv')
df['hv'] = df['hv']
df.set_index('hv', inplace=True)
df = df.applymap(lambda s: str(s))
hv_mapping = dict()
for i, year in enumerate([2010, 2011, 2012]):
    df_temp = df.iloc[:, 2*i:(2*i+2)]
    df_temp.columns = ['pmt', 'vip']
    hv_mapping[year] = {k: v for k, v in df_temp.to_dict('index').items() if '-1' not in list(v.values())}
hv_mapping[2013] = hv_mapping[2012]
# print(hv_mapping)





exit()


coll = client.sphere_telemetry.hv

df = pd.read_csv(DATADIR / 'currents_corrected.txt')
for i, ser in tqdm(df.iterrows(), total=len(df.index)):
    year = ser['year']
    hh, mm, ss = str(ser['HHMMSS']).split(':')
    dt = datetime(year=year, month=ser['month'], day=ser['day'], hour=int(hh), minute=int(mm), second=int(ss))
    currents = {k[3:]: v for k, v in ser.drop(labels=['year', 'month', 'day', 'flight', 'HHMMSS']).to_dict().items()}
    coll.insert_one({
        'utc_time': dt,
        'I_per_hv': currents,
    })
