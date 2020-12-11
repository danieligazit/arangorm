from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class Result():

    def _load(self, data) -> Any:
        pass


@dataclass
class ValueResult(Result):
    def _load(self, data) -> Any:
        return data


@dataclass
class ListResult(Result):
    inner_result: Result

    def _load(self, data) -> Any:
        return list(map(self.inner_result._load, data))


@dataclass
class DocumentResult(Result):
    def _load(self, data) -> Any:
        collection_name = data['_id'].split('/')[0]
        return data

    def __getitem__(self, _item):
        return VALUE_RESULT


@dataclass
class DictResult(Result):
    display_name_to_result: Dict[str, Result]

    def _load(self, data) -> Any:
        return {key: self.display_name_to_result[key]._load(value) for key, value in data.items()}

    def __getitem__(self, item):
        return self.display_name_to_result[item]


VALUE_RESULT = ValueResult()
DOCUMENT_RESULT = DocumentResult()


@dataclass
class AnyResult(Result):
    inner_result: List[Result]

    def _load(self, data) -> Any:
        return DOCUMENT_RESULT._load(data)
