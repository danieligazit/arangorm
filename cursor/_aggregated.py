from dataclasses import dataclass
from typing import Union, Dict

from _query import Aliased
from _result import VALUE_RESULT, Result
from _stmt import Stmt
from cursor._grouped import Grouped
from cursor._selected import Selected


@dataclass
class Aggregated(Grouped, Selected, Aliased):
    group: Union[Grouped, Selected]
    func: str

    def _to_group_stmt(self, prefix: str, collected: str, alias_to_result: Dict[str, Result]) -> Stmt:
        stmt = self.group._to_group_stmt(prefix, collected=collected, alias_to_result=alias_to_result)
        query_str, bind_vars = stmt.expand()
        alias_to_result.update(stmt.alias_to_result)
        return Stmt(f'''{self.func}({query_str})''', bind_vars, result=VALUE_RESULT, aliases=self.aliases,
                    alias_to_result=alias_to_result)

    def _to_select_stmt(self, prefix: str, relative_to: str, alias_to_result: Dict[str, Result]) -> Stmt:
        stmt = self.group._to_select_stmt(prefix, relative_to=relative_to, alias_to_result=alias_to_result)
        query_str, bind_vars = stmt.expand()
        alias_to_result.update(stmt.alias_to_result)
        return Stmt(f'''{self.func}({query_str})''', bind_vars, result=VALUE_RESULT, alias_to_result=alias_to_result)

    def __str__(self) -> str:
        return f'{self.func.lower()}_{self.group}'
