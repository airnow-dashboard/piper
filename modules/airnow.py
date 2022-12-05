import datetime
import glob
import csv
import json
from typing import List
from datetime import timedelta

from modules.common import Source, Record, SourcePath


class AirNowRecord(Record):

    # CREATE TABLE public.pm25_measurements (
    #     datetime timestamp not null,
    #     location varchar(100) not null,
    #     aqi numeric null,
    #     aqi_cat varchar(30) null,
    #     conc numeric null,
    #     PRIMARY KEY(datetime, location)
    # ) PARTITION BY RANGE (datetime);
    fields = ["location", "datetime", "aqi", "aqi_cat", "conc"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class HistoricalSource(Source):
    def __init__(self, csv_file):
        self.csv_file = csv_file

    def read(self) -> List[AirNowRecord]:
        def parse(row) -> AirNowRecord:
            return AirNowRecord(
                location=row.get("Site"),
                datetime=row.get("Date (LT)"),
                aqi=row.get("AQI"),
                aqi_cat=row.get("AQI Category"),
                conc=row.get("Raw Conc.")
            )

        with open(self.csv_file) as f:
            rows = list(csv.DictReader(f))
        return [parse(row) for row in rows]


class CurrentSource(Source):
    def __init__(self, json_file):
        self.json_file = json_file
        self.step_increment_hours = 1  # values are recorded every 1 hour
        self.datetime_format = "%m/%d/%Y %I:%M:%S %p"

    def read(self) -> List[AirNowRecord]:
        def parse(raw_obj) -> List[AirNowRecord]:
            records = []
            for (location, value) in raw_obj.items():
                for monitor in value['monitors']:
                    if monitor['parameter'] != "PM2.5":
                        continue
                    start_time = datetime.datetime.strptime(monitor.get('beginTimeLT'), self.datetime_format)
                    aqis = monitor.get('aqi')
                    aqi_cats = monitor.get('aqiCat')
                    concs = monitor.get('conc')

                    for idx in range(len(aqis)):
                        records.append(AirNowRecord(
                            location=location,
                            datetime=start_time + (idx * timedelta(hours=self.step_increment_hours)),
                            aqi=aqis[idx],
                            aqi_cat=aqi_cats[idx],
                            conc=concs[idx]
                        ))
            return records

        with open(self.json_file) as f:
            raw_obj = json.load(f)

        return parse(raw_obj)


class AirNowSourcePath(SourcePath):
    def __init__(self, source_path, matching_glob):
        super().__init__(source_path)
        self.matching_glob = matching_glob

    def list(self):
        return glob.iglob(self.source_path + self.matching_glob, recursive=True)
