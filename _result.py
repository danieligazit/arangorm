import inspect
import itertools
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Any

from _collection import Collection


@dataclass
class Result:
    def _load(self, data, collection_definition: Dict[str, Collection], db: 'DB') -> Any:
        pass


@dataclass
class ValueResult(Result):
    def _load(self, data, collection_definition: Dict[str, Collection], db: 'DB') -> Any:
        return data

    def __getitem__(self, item):
        return VALUE_RESULT


@dataclass
class ListResult(Result):
    inner_result: Result

    def _load(self, data, collection_definition: Dict[str, Collection], db: 'DB') -> Any:
        return list(map(self.inner_result._load, data, itertools.repeat(collection_definition), itertools.repeat(db)))

    def __getitem__(self, item):
        return self.inner_result


@dataclass
class DocumentResult(Result):
    def _load(self, data, collection_definition: Dict[str, Collection], db:'DB') -> Any:
        if not data:
            return {}
        collection_name = data['_id'].split('/')[0]
        return collection_definition.get(collection_name, ValueResult).document_type._load(data,
                                                                                           collection_definition)

    def __getitem__(self, _item):
        return VALUE_RESULT


@dataclass
class DictResult(Result):
    display_name_to_result: Dict[str, Result]

    def _load(self, data, collection_definition: Dict[str, Collection]) -> Any:
        return {key: self.display_name_to_result[key]._load(value, collection_definition) for key, value in
                data.items()}

    def __getitem__(self, item):
        return self.display_name_to_result[item]


VALUE_RESULT = ValueResult()
DOCUMENT_RESULT = DocumentResult()


@dataclass
class AnyResult(Result):
    inner_result: List[Result]

    def _load(self, data, collection_definition: Dict[str, Collection]) -> Any:
        return DOCUMENT_RESULT._load(data, collection_definition)
