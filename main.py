import sys
from os import environ
import logging
from multiprocessing import Pool

import fire

from modules.airnow import HistoricalSource, CurrentSource, AirNowSourcePath
from modules.common import PostgresSink

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

multiprocess_thread_count = 5

AIRNOW_DB_HOST = environ.get('AIRNOW_DB_HOST', 'localhost')
AIRNOW_DB_USER = environ.get('AIRNOW_DB_USER', 'airnow_admin')
AIRNOW_DB_PASSWORD = environ.get('AIRNOW_DB_PASSWORD', 'changeme')

# initialize postgres sinks
pm25_sink = PostgresSink(host=AIRNOW_DB_HOST, user=AIRNOW_DB_USER, password=AIRNOW_DB_PASSWORD,
                         db='airnow', table='pm25_measurements')
city_sink = PostgresSink(host=AIRNOW_DB_HOST, user=AIRNOW_DB_USER, password=AIRNOW_DB_PASSWORD,
                         db='airnow', table='cities')


def main(source_path, type):
    """A simple pipeline to ingest AirNow data from files into postgres database.

    Args:
        source_path: Filesystem path containing the fetched files.
        type: Type of the files, can only be either 'historical' or 'current'.
    """
    if type not in ('historical', 'current'):
        raise TypeError("Unknown type '{}'".format(type))

    if type == "historical":
        logging.info("Running 'historical' pipeline.")
        logging.info("Getting files from {}...".format(source_path))
        source_path = AirNowSourcePath(source_path, matching_glob='**/*PM2.5*.csv')
        source_files = list(source_path.list())
        logging.info("Fetched files: {}".format(source_files))
        sources = [HistoricalSource(s) for s in source_files]

        def process(source):
            print("Processing {}...".format(source))
            records = source.read()
            pm25_sink.write(records, upsert_columns=('datetime', 'location'))

        with Pool(multiprocess_thread_count) as p:
            p.map(process, sources)
        logging.info("Nothing else to do in the pipeline.")

    elif type == "current":
        logging.info("Running 'current' pipeline.")
        logging.info("Getting files from {}...".format(source_path))
        source_path = AirNowSourcePath(source_path, matching_glob='**/*.json')
        source_files = list(source_path.list())
        logging.info("Fetched files: {}".format(source_files))
        sources = [CurrentSource(s) for s in source_files]

        for source in sources:
            print("Processing {}...".format(source))
            city_records, pm25_records = source.read()
            city_sink.write(city_records, upsert_columns=('location',))
            pm25_sink.write(pm25_records, upsert_columns=('datetime', 'location'))
        logging.info("Nothing else to do in the pipeline.")

if __name__ == '__main__':
    logging.info("Starting piper.")
    fire.Fire(main)
    logging.info("Finished piper.")
