from abc import ABC, abstractmethod
from typing import List, Union

import psycopg2


class Record:
    """A class to standardize records to be written into sink.

    """
    fields = []

    def __init__(self, **kwargs):
        self.__data = {k: v for k, v in kwargs.items() if k in self.fields}

    def __repr__(self):
        return str(self.__data)

    def get_fields(self):
        return self.fields

    def get_values(self):
        return [self.__data[field] if field in self.__data else None for field in self.fields]


class Source(ABC):
    @abstractmethod
    def read(self) -> List[Record]:
        pass


class Sink(ABC):
    @abstractmethod
    def write(self, record: Record):
        pass


class SourcePath(ABC):

    def __init__(self, source_path):
        self.source_path = source_path

    @abstractmethod
    def list(self):
        pass


class PostgresSink(Sink):
    def __init__(self, host: str, db: str, table: str, user: str, password: str):
        self.host = host
        self.db = db
        self.table = table
        self.user = user
        self.password = password
        self.conn = psycopg2.connect(
            "dbname='{}' user='{}' host='{}' password='{}'".format(self.db, self.user, self.host, self.password))

    def write(self, record: Union[Record, List[Record]], upsert_columns=None):
        is_list = type(record) == list

        # generates upsert query
        if is_list:
            fields = record[0].get_fields()
        else:
            fields = record.get_fields()
        query = "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ".format(
            self.table,
            ', '.join(fields),
            ', '.join(["%s" for _ in fields])
        )
        if not upsert_columns:
            query += "DO NOTHING"
        else:
            query += "({}) DO UPDATE SET {}".format(
                ", ".join(upsert_columns),
                ", ".join(["{} = EXCLUDED.{}".format(field, field) for field in fields])
            )

        cur = self.conn.cursor()

        if is_list:
            batch_size = 200
            for batch_start_idx in range(0, len(record), batch_size):
                print(batch_start_idx, batch_start_idx + batch_size)
                cur.executemany(
                    query,
                    [r.get_values() for r in record[batch_start_idx:batch_start_idx + batch_size]]
                )
        else:
            cur.execute(query, record.get_values())

        # commit the changes to the database
        self.conn.commit()
        # close communication with the database
        cur.close()
