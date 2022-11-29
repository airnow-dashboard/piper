import os

from modules.airnow import HistoricalSource, CurrentSource, PostgresSink, AirNowRecord

source_path = "/Users/solosynth1/workspace/pdd-data-analytics/research-assistant/airnow-scraper/output/dos/"


def main():
    files = os.listdir(source_path+'current/Beijing/')
    print(files)
    records = HistoricalSource(source_path + 'historical/Beijing/2021/Beijing_PM2.5_2021_YTD.csv').read()
    print(records[0])
    #
    print(CurrentSource(source_path + 'current/Beijing/Beijing-PM2.5.json').read())
    sink = PostgresSink(db='airnow', host='104.198.50.100', user='airnow_admin', password='changeme', table='pm25_measurements')
    # sink.write(records[0])
    sink.write_multiple(records)


if __name__ == '__main__':
    main()
