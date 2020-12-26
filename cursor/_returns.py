from dataclasses import dataclass, field
from inspect import isclass
from typing import List

from _result import Result, DOCUMENT_RESULT, VALUE_RESULT


@dataclass
class Returns:
    attribute_return: str = field(default='', init=False)
    attribute_return_list: List[str] = field(default_factory=list, init=False)

    def _get_result(self, result: Result) -> Result:
        if len(self.attribute_return_list) > 0 and isclass(result):
            result = DOCUMENT_RESULT

        for attribute in self.attribute_return_list:
            if hasattr(result, '__getitem__'):
                result = result[attribute]
            else:
                result = VALUE_RESULT

        return result

    def __getattr__(self, attr: str) :
        self.attribute_return += '.' + attr
        self.attribute_return_list.append(attr)
        return self
