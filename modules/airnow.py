import glob
import csv
import json
from typing import List, Tuple
from datetime import datetime, timedelta

from modules.common import Source, Record, SourcePath


class CityRecord(Record):
    fields = ['location', 'latitude', 'longitude', 'latlon']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


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

    def __repr__(self):
        return self.csv_file

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
                    aqi=-999,
                    aqi_cat='N/A',
                    conc=-999
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
        self.aqi_cat_map = {
            None: 'N/A',
            1: 'Good',
            2: 'Moderate',
            3: 'Unhealthy for Sensitive Groups',
            4: 'Unhealthy',
            5: 'Very Unhealthy',
            6: 'Hazardous'
        }

    def __repr__(self):
        return self.json_file

    def parse(self, raw_obj) -> Tuple[List[CityRecord], List[AirNowRecord]]:
        pm25_records = []
        cities_records = []
        for (location, value) in raw_obj.items():
            longitude, latitude = value['coordinates']
            latlon = "{},{}".format(latitude, longitude)
            cities_records.append(CityRecord(
                location=location,
                longitude=longitude,
                latitude=latitude,
                latlon=latlon
            ))
            for monitor in value['monitors']:
                if monitor['parameter'] != "PM2.5":
                    continue
                start_time = datetime.strptime(monitor.get('beginTimeLT'), self.datetime_format)
                aqis = monitor.get('aqi')
                aqi_cats = monitor.get('aqiCat')
                concs = monitor.get('conc')

                for idx in range(len(aqis)):
                    pm25_records.append(AirNowRecord(
                        location=location,
                        datetime=start_time + (idx * timedelta(hours=self.step_increment_hours)),
                        aqi=aqis[idx] if aqis[idx] is not None else -999,
                        aqi_cat=self.aqi_cat_map[aqi_cats[idx]],
                        conc=concs[idx] if concs[idx] is not None else -999
                    ))

        return cities_records, pm25_records

    def read(self) -> Tuple[List[CityRecord], List[AirNowRecord]]:
        with open(self.json_file) as f:
            raw_obj = json.load(f)
        return self.parse(raw_obj)


class AirNowSourcePath(SourcePath):
    def __init__(self, source_path, matching_glob):
        super().__init__(source_path)
        self.matching_glob = matching_glob

    def list(self):
        return glob.iglob(self.source_path + self.matching_glob, recursive=True)
