from abc import abstractmethod, ABC
from datetime import datetime
from typing import List

from collection import Collection

DEFAULT_DUMP_KEYS = ['_key', '_id', '_rev']


class Document(ABC):
    def __init__(self, _key=None, _rev=None, _id=None):
        self._key = _key
        self._rev = _rev
        self._id = _id

    @abstractmethod
    def get_collection(self) -> Collection:
        pass

    @classmethod
    def _load(cls, d: dict) -> 'Document':
        return cls(**d)

    def _dump(self) -> dict:
        return self._dump_from_dict(vars(self))

    @staticmethod
    def _dump_from_dict(result, keys: List['str'] = None):
        if keys is None:
            keys = DEFAULT_DUMP_KEYS

        for key, value in list(result.items()):
            if key in keys and not value:
                result.pop(key)

            if isinstance(value, datetime):
                result[key] = str(value)

        return result

    def __repr__(self) -> str:
        return f'{type(self)}{vars(self)}'


class Edge(Document):
    def __init__(self, _key=None, _rev=None, _id=None, _from=None, _to=None):
        super().__init__(_key, _rev, _id)
        self._from = _from
        self._to = _to

    def _dump(self) -> dict:
        return self._dump_from_dict(vars(self), keys=['_key', '_id', '_rev', '_from', '_to'])
