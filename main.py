from multiprocessing import Pool

import fire

from modules.airnow import HistoricalSource, CurrentSource, AirNowSourcePath
from modules.common import PostgresSink


multiprocess_thread_count = 5
# initialize postgres
sink = PostgresSink(db='airnow', host='35.225.50.70', user='airnow_admin', password='changeme',
                    table='pm25_measurements')


def process(source):
    print("Processing {}...".format(source))
    records = source.read()
    sink.write(records, upsert_columns=('datetime', 'location'))


def main(source_path, type):
    """A simple pipeline to ingest AirNow data from files into postgres database.

    Args:
        source_path: Filesystem path containing the fetched files.
        type: Type of the files, can only be either 'historical' or 'current'.
    """
    if type not in ('historical', 'current'):
        raise TypeError("Unknown type '{}'".format(type))

    if type == "historical":
        source_path = AirNowSourcePath(source_path, matching_glob='**/*PM2.5*.csv')
        sources = [HistoricalSource(s) for s in source_path.list()]
        with Pool(multiprocess_thread_count) as p:
            p.map(process, sources)
    elif type == "current":
        source_path = AirNowSourcePath(source_path, matching_glob='**/*.json')
        sources = [CurrentSource(s) for s in source_path.list()]
        with Pool(multiprocess_thread_count) as p:
            p.map(process, sources)


if __name__ == '__main__':
    fire.Fire(main)
