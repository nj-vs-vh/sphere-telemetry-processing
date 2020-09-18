import pandas as pd
from pathlib import Path
from operator import attrgetter
from warnings import warn


DATUM_DIR = Path('.\\data\\datum_tables')


def datum_datetime(df: pd.DataFrame):
    """Parse datetime object from several columns in datum

    Args:
        df (pd.DataFrame): read from datum .csv file

    Returns:
        pd.Series: parsed datetime64 Series
    """
    datetime = df.loc[:, 'time'].astype(str).str.cat(
        others=[
            df.loc[:, 'year'].astype(str),
            df.loc[:, 'month'].astype(str).apply(lambda s: s.rjust(2, '0')),
            df.loc[:, 'day'].astype(str).apply(lambda s: s.rjust(2, '0'))
        ],
        sep=' '
    )

    datetime = pd.to_datetime(datetime, format=r'%H%M%S %Y %m %d')
    datetime.name = 'datetime'

    return datetime


def read_datum_for_year(year):
    """Read specific datum and prepare datetime index"""
    datum_filename = DATUM_DIR / f'datum_{year}_sec.csv'
    try:
        df = pd.read_csv(datum_filename)
    except FileNotFoundError:
        warn(f'No datum file found for the year {year}; check for \'{datum_filename}\'')
        return None
    df.insert(0, 'datetime', datum_datetime(df))
    df.set_index('datetime', inplace=True, drop=False)
    df.index.name = 'local_dt_index'
    df.drop(columns=['year', 'month', 'day', 'time'], inplace=True)
    return df


def telemetry_data_at(datetimes, columns):
    """Main function to extract telemetry data for specific datetimes from datum tables.

    Args:
        datetimes (pd.Series with Timestamps or single Timestamp): one or several query datetimes
        columns (list of str): list of columns to retrieve from datum tables;
    Returns:
        pd.DataFrame with retrieved columns at queried dates, interpolated where datetimes doesn't match
    """
    if not isinstance(datetimes, pd.Series):
        datetimes = pd.to_datetime(pd.Series(data=datetimes))

    query = pd.DataFrame(index=datetimes, columns=columns)

    years = datetimes.copy().map(attrgetter('year'))
    for year in years.unique():
        datum = read_datum_for_year(year)
        if datum is None:
            continue
        if columns:
            datum = datum[columns]

        datetimes_in_year = datetimes[years == year]

        query_year = pd.DataFrame(index=datetimes_in_year, columns=columns)
        datum = datum.append(query_year).sort_index()
        datum.interpolate(method='time', inplace=True)
        datum = datum[~datum.index.duplicated(keep='first')]

        query.loc[datetimes_in_year, :] = datum.loc[datetimes_in_year, :]

    return query


if __name__ == "__main__":
    print(telemetry_data_at(['2013-03-09 15:16:17', '2012-03-09 13:16:29'], columns=['H']))
