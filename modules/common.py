from abc import ABC, abstractmethod
from typing import List, Union, Tuple

import psycopg2
import psycopg2.extras


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

    def contains_field(self, field):
        if field not in self.fields:
            raise KeyError("field {} does not exist in the record".format(field))

    def get_value(self, field):
        self.contains_field(field)
        return self.__data[field]

    def get_field_index(self, field):
        self.contains_field(field)
        return self.fields.index(field)


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
        self.execute_page_size = 500

    def write(self, record: Union[Record, List[Record]], upsert_columns: Tuple = None):
        is_list = type(record) == list

        if is_list:
            data = record
        else:
            data = [record]

        # generates upsert query
        fields = data[0].get_fields()

        query = "INSERT INTO {} ({}) VALUES %s ON CONFLICT ".format(
            self.table,
            ', '.join(fields),
        )
        if not upsert_columns:
            query += "DO NOTHING"
        else:
            query += "({}) DO UPDATE SET {}".format(
                ", ".join(upsert_columns),
                ", ".join(["{} = EXCLUDED.{}".format(field, field) for field in fields])
            )
            # data = self.deduplicate_by_columns(data, upsert_columns)

        cur = self.conn.cursor()

        psycopg2.extras.execute_values(
            cur, query, [r.get_values() for r in data],
            template=None,
            page_size=self.execute_page_size,
            fetch=False
        )

        # commit the changes to the database
        self.conn.commit()
        # close communication with the database
        cur.close()
