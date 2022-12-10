from multiprocessing import Pool

import fire

from modules.airnow import HistoricalSource, CurrentSource, AirNowSourcePath
from modules.common import PostgresSink


multiprocess_thread_count = 5

# initialize postgres sinks
pm25_sink = PostgresSink(db='airnow', host='35.225.50.70', user='airnow_admin', password='changeme',
                    table='pm25_measurements')
city_sink = PostgresSink(db='airnow', host='35.225.50.70', user='airnow_admin', password='changeme',
                    table='cities')


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

        def process(source):
            print("Processing {}...".format(source))
            records = source.read()
            pm25_sink.write(records, upsert_columns=('datetime', 'location'))

        with Pool(multiprocess_thread_count) as p:
            p.map(process, sources)

    elif type == "current":
        source_path = AirNowSourcePath(source_path, matching_glob='**/*.json')
        sources = [CurrentSource(s) for s in source_path.list()]

        for source in sources:
            print("Processing {}...".format(source))
            city_records, pm25_records = source.read()
            city_sink.write(city_records, upsert_columns=('location',))
            pm25_sink.write(pm25_records, upsert_columns=('datetime', 'location'))


if __name__ == '__main__':
    fire.Fire(main)
