from abc import abstractmethod, ABC
from datetime import datetime
from typing import List, Generic, TypeVar

from collection import EdgeCollection
from query import DocumentQuery
from result import Result

DEFAULT_DUMP_KEYS = ['_key', '_id', '_rev']

T = TypeVar('T', bound='Collection')


class Document(ABC, Result):
    def __init__(self, _key=None, _rev=None, _id=None):
        self._key = _key
        self._rev = _rev
        self._id = _id

    @classmethod
    @abstractmethod
    def get_collection(cls) -> T:
        pass

    @classmethod
    def _load(cls, d: dict) -> T:
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

    def _set_meta(self, _id: str, _key: str, _rev: str):
        self._id = _id
        self._key = _key
        self._rev = _rev

    def __repr__(self) -> str:
        return f'{type(self)}{vars(self)}'

    @classmethod
    def match(cls, *matchers, **key_value_match):
        dq = DocumentQuery(collection=cls.get_collection())
        dq.match(*matchers, **key_value_match)

        return dq


class Edge(Document):
    def __init__(self, _key=None, _rev=None, _id=None, _from=None, _to=None):
        super().__init__(_key, _rev, _id)
        self._from = _from
        self._to = _to

    def _dump(self) -> dict:
        return self._dump_from_dict(vars(self), keys=['_key', '_id', '_rev', '_from', '_to'])

    @classmethod
    @abstractmethod
    def get_collection(cls) -> 'EdgeCollection':
        pass

    # def _set_meta(self, _id: str, _key: str, _rev: str, _from: str = None, _to = None, _to: str):
    #     super()._set_meta(_id, _key, _rev)
    #     self._from = _from
    #     self._to = _to
