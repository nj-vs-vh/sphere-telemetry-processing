from pymongo import MongoClient

from tqdm import tqdm

# assuming Mongo is running as mongod process/service and listening on localhost port 27017
# for installation see https://docs.mongodb.com/manual/administration/install-community/
# for restoring data from dump see README.md
client = MongoClient()
datum = client.sphere_telemetry.from_datum_tables
master = client.sphere_telemetry.master


pipeline = [
    {'$match': {'utc_dt': {'$exists': True}}},
    {'$project': {'local_dt': 0, 'GPS_stamp': 0, '_id': 0}},
    {'$addFields': {'from_datum': {'$literal': True}}},
]

total = datum.aggregate(pipeline + [{"$count": "N"}]).next()['N']

for doc in tqdm(datum.aggregate(pipeline), total=total):
    master.update_one(
        filter={'utc_dt': doc['utc_dt']},
        update={"$setOnInsert": doc},
        upsert=True
    )
