from dataclasses import dataclass
from typing import List, Dict


@dataclass
class Result:
    pass


@dataclass
class ValueResult(Result):
    pass


@dataclass
class ListResult(Result):
    inner_result: Result


@dataclass
class AnyResult(Result):
    inner_result: List[Result]


@dataclass
class DocumentResult(Result):
    pass

    def __getitem__(self, _item):
        return VALUE_RESULT


@dataclass
class DictResult(Result):
    display_name_to_result: Dict[str, Result]

    def __getitem__(self, item):
        return self.display_name_to_result[item]


VALUE_RESULT = ValueResult()
DOCUMENT_RESULT = DocumentResult()
