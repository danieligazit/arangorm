from dataclasses import dataclass
from typing import Any

from _stmt import Stmt
from cursor._var import Var
from cursor.filters._filter import Filter
from cursor._operator import Operator


@dataclass
class AttributeFilter(Filter):
    attribute: str
    operator: Operator
    compare_value: Any

    def _to_filter_stmt(self, prefix: str, relative_to: str) -> Stmt:

        if isinstance(self.compare_value, Var):
            return Stmt(f'FILTER {relative_to}.{self.attribute} {self.operator} {self.compare_value._name}{self.compare_value.attribute_return}',
                        {})
        return Stmt(f'FILTER {relative_to}.{self.attribute} {self.operator} @{prefix} ', {prefix: self.compare_value})