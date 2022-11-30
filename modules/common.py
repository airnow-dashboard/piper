from abc import ABC, abstractmethod
from typing import List


class Record:
    @abstractmethod
    def get(self):
        pass


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
