import fire

from modules.airnow import HistoricalSource, CurrentSource, AirNowSourcePath
from modules.common import PostgresSink


def main(source_path, type):
    """A simple pipeline to ingest AirNow data from files into postgres database.

    Args:
        source_path: Filesystem path containing the fetched files.
        type: Type of the files, can only be either 'historical' or 'current'.
    """
    if type not in ('historical', 'current'):
        raise TypeError("Unknown type '{}'".format(type))

    # initialize postgres
    sink = PostgresSink(db='airnow', host='35.225.50.70', user='airnow_admin', password='changeme', table='pm25_measurements')

    if type == "historical":
        source = AirNowSourcePath(source_path, matching_glob='**/*PM2.5*.csv')
        file_paths = source.list()
        for file_path in file_paths:
            print("Processing {}...".format(file_path))
            records = HistoricalSource(file_path).read()
            sink.write(records, upsert_columns=('datetime', 'location'))
    elif type == "current":
        source = AirNowSourcePath(source_path, matching_glob='**/*.json')
        file_paths = source.list()
        for file_path in file_paths:
            print("Processing {}...".format(file_path))
            records = CurrentSource(file_path).read()
            sink.write(records, upsert_columns=('datetime', 'location'))


if __name__ == '__main__':
    fire.Fire(main)
