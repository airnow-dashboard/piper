import fire

from modules.airnow import HistoricalSource, CurrentSource, PostgresSink, AirNowRecord, AirNowSourcePath

# source_path = "/Users/solosynth1/workspace/pdd-data-analytics/research-assistant/airnow-scraper/output/dos/"

def main(source_path, type):
    """A simple pipeline to ingest AirNow data from files into postgres database.

    Args:
        source_path: Filesystem path containing the fetched files.
        type: Type of the files, can only be either 'historical' or 'current'.
    """
    if type not in ('historical', 'current'):
        raise TypeError("Unknown type '{}'".format(type))

    # initialize postgres
    sink = PostgresSink(db='airnow', host='localhost', user='postgres', password='changeme', table='pm25_measurements')

    if type == "historical":
        source = AirNowSourcePath(source_path, matching_glob='**/*PM2.5*YTD.csv')
        file_paths = source.list()
        for file_path in file_paths:
            records = HistoricalSource(file_path).read()
            sink.write_multiple(records)
    elif type == "current":
        source = AirNowSourcePath(source_path, matching_glob='**/*PM2.5.json')
        file_paths = source.list()

    # print(records[0])
    # #
    # print(CurrentSource(source_path + 'current/Beijing/Beijing-PM2.5.json').read())
    # sink = PostgresSink(db='airnow', host='localhost', user='postgres', password='changeme', table='pm25_measurements')
    # # sink.write(records[0])
    # sink.write_multiple(records)


if __name__ == '__main__':
    fire.Fire(main)
