from pymongo import MongoClient
from datetime import datetime
from typing import Any

client = MongoClient()
tele = client.sphere_telemetry.master


def next_to_dt(field: str, dt: datetime, direction: int) -> Any:
    cmp_ = "$lte" if direction == -1 else "$gte"
    try:
        doc = tele.aggregate([
            {"$match": {"utc_dt": {cmp_: dt}, field: {"$exists": True}}},
            {"$sort": {"utc_dt": direction}}
        ]).next()
        # print(doc['_id'])  # for debugging
        return doc['utc_dt'], doc[field]
    except StopIteration:
        raise IndexError(f"Requested dt={dt} seems to be out of bounds!")


def interpolate_field(field: str, dt: datetime, kind: str = 'linear') -> Any:
    """Get linearly interpolated value of 'field' from database at arbitrary datetime 'dt'

    Supported interpolation types: linear, nearest
    """
    ldt, lval = next_to_dt(field, dt, -1)
    rdt, rval = next_to_dt(field, dt, 1)
    ldelta = (dt - ldt).total_seconds()
    rdelta = (rdt - dt).total_seconds()
    if kind == 'linear':
        return lval + (rval - lval) * ldelta / (ldelta + rdelta)
    elif kind == 'nearest':
        return lval if ldelta < rdelta else rval
    else:
        raise ValueError(f"Invalid interpolation kind '{kind}'")


if __name__ == "__main__":
    dt = datetime.strptime("2012-03-16 06:06:06", r"%Y-%m-%d %X")
    print(interpolate_field('compass', dt))

    dt = datetime.strptime("2013-03-14 08:36:06", r"%Y-%m-%d %X")
    print(interpolate_field('H_m', dt))
