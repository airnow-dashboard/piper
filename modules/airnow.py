import glob
import csv
import json
from typing import List
from datetime import datetime, timedelta

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
        self.datetime_format = "%Y-%m-%d %I:%M %p"

    def parse(self, rows) -> List[AirNowRecord]:
        records_map = {}
        # keys = []
        for row in rows:
            key = row.get("Date (LT)")
            # this is for deduplication as there will be times when a single timestamp can have multiple records
            if key not in records_map:
                records_map[key] = AirNowRecord(
                    location=row.get("Site"),
                    datetime=row.get("Date (LT)"),
                    aqi=row.get("AQI"),
                    aqi_cat=row.get("AQI Category"),
                    conc=row.get("Raw Conc.")
                )

        start_year = datetime.strptime(sorted(records_map.keys())[0], self.datetime_format).year
        start_datetime = datetime(year=start_year, month=1, day=1, hour=1, minute=0)

        # if start year is not current year
        if start_year != datetime.now().year:
            # create a whole year's timeframe
            end_datetime = datetime(start_datetime.year + 1, month=1, day=1, hour=1, minute=0)
        # if not,
        else:
            # create to current end of record
            end_datetime = datetime.strptime(sorted(records_map.keys())[-1], self.datetime_format)

        current_datetime = start_datetime
        while current_datetime <= end_datetime:
            current_datetime_str = datetime.strftime(current_datetime, self.datetime_format)
            if current_datetime_str not in records_map:
                records_map[current_datetime_str] = AirNowRecord(
                    location=rows[0].get("Site"),
                    datetime=current_datetime_str,
                    aqi=None,
                    aqi_cat=None,
                    conc=None
                )
            current_datetime += timedelta(hours=1)
        return list(records_map.values())

    def read(self) -> List[AirNowRecord]:
        with open(self.csv_file) as f:
            rows = list(csv.DictReader(f))

        return self.parse(rows)


class CurrentSource(Source):
    def __init__(self, json_file):
        self.json_file = json_file
        self.step_increment_hours = 1  # values are recorded every 1 hour
        self.datetime_format = "%m/%d/%Y %I:%M:%S %p"

    def parse(self, raw_obj) -> List[AirNowRecord]:
        records = []
        for (location, value) in raw_obj.items():
            for monitor in value['monitors']:
                if monitor['parameter'] != "PM2.5":
                    continue
                start_time = datetime.strptime(monitor.get('beginTimeLT'), self.datetime_format)
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

    def read(self) -> List[AirNowRecord]:
        with open(self.json_file) as f:
            raw_obj = json.load(f)

        return self.parse(raw_obj)


class AirNowSourcePath(SourcePath):
    def __init__(self, source_path, matching_glob):
        super().__init__(source_path)
        self.matching_glob = matching_glob

    def list(self):
        return glob.iglob(self.source_path + self.matching_glob, recursive=True)
