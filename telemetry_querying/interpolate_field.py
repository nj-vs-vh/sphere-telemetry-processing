from pymongo import MongoClient
from pymongo.collection import Collection
from datetime import datetime
from typing import Any, List


def interpolate_field(coll: Collection, field: str, dts: List[datetime], kind: str = 'linear') -> Any:
    """Get linearly interpolated value of 'field' from database at arbitrary datetime 'dt'

    Supported interpolation types: linear, nearest
    """

    def next_to_dt(field: str, dt: datetime, direction: int) -> Any:
        cmp_ = "$lte" if direction == -1 else "$gte"
        try:
            doc = coll.aggregate([
                {"$match": {"utc_dt": {cmp_: dt}, field: {"$exists": True}}},
                {"$sort": {"utc_dt": direction}}
            ]).next()
            # print(doc['_id'])  # for debugging
            return doc['utc_dt'], doc[field]
        except StopIteration:
            raise IndexError(f"Requested dt={dt} seems to be out of bounds!")

    def interp(dt, ldt, rdt, lval, rval, kind):
        ldelta = (dt - ldt).total_seconds()
        rdelta = (rdt - dt).total_seconds()
        if kind == 'linear':
            return lval + (rval - lval) * ldelta / (ldelta + rdelta)
        elif kind == 'nearest':
            return lval if ldelta < rdelta else rval
        else:
            raise ValueError(f"Invalid interpolation kind '{kind}'")

    if coll.find_one(filter={field: {"$exists": True}}) is None:
        raise ValueError(f"Invalid field '{field}'")

    dts = sorted(dts)  # , key=lambda dt: dt.total_seconds)
    interp_field = []

    startdt, lval = next_to_dt(field, dts[0], -1)
    enddt, _ = next_to_dt(field, dt[-1], 1)

    interp_idx = 0
    ldt = startdt
    for doc in coll.aggregate([
        {"$match": {"utc_dt": {"$gte": startdt, "$lte": enddt}, field: {"$exists": True}}},
        {"$sort": {"utc_dt": 1}},
        {"$project": {"utc_dt": True, field: True}}
    ]):
        if doc['utc_dt'] <= dts[interp_idx]:
            ldt = doc['utc_dt']
            lval = doc[field]
        else:
            rdt = doc['utc_dt']
            rval = doc[field]
            while interp_idx < len(dts) and dts[interp_idx] <= rdt:
                interp_field.append(interp(dts[interp_idx], ldt, rdt, lval, rval, kind))
                interp_idx += 1
    return interp_field


if __name__ == "__main__":
    # special collection for internal testing! do not run
    client = MongoClient()
    test = client.sphere_telemetry.interptest
    dt = [
        datetime.strptime("2000-01-01 00:00:00", r"%Y-%m-%d %X"),
        datetime.strptime("2000-01-01 00:01:00", r"%Y-%m-%d %X"),
        datetime.strptime("2000-01-01 00:02:00", r"%Y-%m-%d %X"),
        datetime.strptime("2000-01-01 00:09:00", r"%Y-%m-%d %X"),
        datetime.strptime("2000-01-01 00:11:00", r"%Y-%m-%d %X"),
        datetime.strptime("2000-01-01 00:25:00", r"%Y-%m-%d %X"),
        datetime.strptime("2000-01-01 00:30:00", r"%Y-%m-%d %X")
    ]
    print(interpolate_field(test, 'b', dt))
