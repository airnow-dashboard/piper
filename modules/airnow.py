import glob
import csv
import json
from typing import List
import datetime

import psycopg2

from modules.common import Source, Record, Sink, SourcePath


class AirNowRecord(Record):

    # CREATE TABLE public.pm25_measurements (
    #     datetime timestamp not null,
    #     location varchar(100) not null,
    #     aqi numeric null,
    #     aqi_cat varchar(30) null,
    #     conc numeric null,
    #     PRIMARY KEY(datetime, location)
    # ) PARTITION BY RANGE (datetime);

    def __init__(self, location, datetime, aqi, aqi_cat, conc):
        self.__data = {
            "location": location,
            "datetime": datetime,
            "aqi": aqi,
            "aqi_cat": aqi_cat,
            "conc": conc
        }

    def get(self):
        return self.__data

    def __repr__(self):
        return str(self.__data)

    def get_fields(self):
        return "(location, datetime, aqi, aqi_cat, conc)"

    def get_values(self):
        # return "('{location}', '{datetime}', {aqi}, {aqi_cat}, {conc})".format_map(self.__data)
        return (
            self.__data["location"],
            self.__data["datetime"],
            self.__data["aqi"],
            self.__data["aqi_cat"],
            self.__data["conc"]
        )


class HistoricalSource(Source):
    def __init__(self, csv_file):
        self.csv_file = csv_file

    def read(self):
        def parse(row) -> dict:
            # 2022-11-01 01:00 AM
            parsed_datetime = datetime.datetime.strptime(row.get("Date (LT)"), "%Y-%m-%d %I:%M %p")
            return {
                "location": row.get("Site"),
                "datetime": row.get("Date (LT)"),
                "aqi": row.get("AQI"),
                "aqi_cat": row.get("AQI Category"),
                "conc": row.get("Raw Conc."),
            }

        with open(self.csv_file) as f:
            rows = list(csv.DictReader(f))
        return [AirNowRecord(**parse(row)) for row in rows]


class CurrentSource(Source):
    def __init__(self, json_file):
        self.json_file = json_file

    def read(self):
        with open(self.json_file) as f:
            raw_obj = json.load(f)

        return raw_obj


class PostgresSink(Sink):
    def __init__(self, host: str, db: str, table: str, user: str, password: str):
        self.host = host
        self.db = db
        self.table = table
        self.user = user
        self.password = password
        self.conn = psycopg2.connect(
            "dbname='{}' user='{}' host='{}' password='{}'".format(self.db, self.user, self.host, self.password))
        print(self.conn)

    def write(self, record: AirNowRecord):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO pm25_measurements {} VALUES {}".format(record.get_fields(), record.get_values()))
        # commit the changes to the database
        self.conn.commit()
        # close communication with the database
        cur.close()

    def write_multiple(self, records: List[AirNowRecord]):
        cur = self.conn.cursor()
        print([r.get_values() for r in records])
        batch_size = 200
        for batch_start_idx in range(0, len(records), batch_size):
            print(batch_start_idx, batch_start_idx+batch_size)
            cur.executemany("INSERT INTO pm25_measurements (location, datetime, aqi, aqi_cat, conc) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", [r.get_values() for r in records[batch_start_idx:batch_start_idx+batch_size]])
        # commit the changes to the database
        self.conn.commit()
        # close communication with the database
        cur.close()


class AirNowSourcePath(SourcePath):
    def __init__(self, source_path, matching_glob):
        super().__init__(source_path)
        self.matching_glob = matching_glob

    def list(self):
        return glob.iglob(self.source_path + self.matching_glob, recursive=True)
