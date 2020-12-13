from abc import abstractmethod, ABC
from datetime import datetime
from typing import List, TypeVar, Any, Dict, Collection

from _query import DocumentQuery
from _result import Result

T = TypeVar('T', bound='Collection')


class Document(ABC, Result):
    INIT_PROPERTIES = ['_key', '_id', '_rev']

    def __init__(self, _key=None, _rev=None, _id=None):
        self._key = _key
        self._rev = _rev
        self._id = _id

    @classmethod
    def _load(cls, d: Any, _: Dict[str, Collection]) -> T:
        return cls(**d)

    def _dump(self) -> dict:
        return self._dump_from_dict(vars(self), self.INIT_PROPERTIES)

    @staticmethod
    def _dump_from_dict(result, keys: List[str]):
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

    @classmethod
    def match(cls, *matchers, **key_value_match):
        dq = DocumentQuery(collection=cls._get_collection())
        dq.match(*matchers, **key_value_match)

        return dq


class Edge(Document):
    INIT_PROPERTIES = ['_key', '_id', '_rev', '_from', '_to']

    def __init__(self, _key=None, _rev=None, _id=None, _from=None, _to=None):
        super().__init__(_key, _rev, _id)
        self._from = _from
        self._to = _to

    def _dump(self) -> dict:
        return self._dump_from_dict(vars(self), keys=self.INIT_PROPERTIES)
