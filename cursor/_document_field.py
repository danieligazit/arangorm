from dataclasses import dataclass, field as dataclass_field
from typing import Union, Dict, Type

from cursor._aliased import Aliased
from _result import VALUE_RESULT, Result, ListResult, DOCUMENT_RESULT
from _stmt import Stmt
from cursor._grouped import Grouped
from cursor._selected import Selected


@dataclass
class Field(Grouped, Selected, Aliased):
    field: Union[str, Type['Document'], Type[object]]
    used_in_by: bool = dataclass_field(default=False)

    def _to_group_stmt(self, prefix: str, collected: str, alias_to_result: Dict[str, Result]) -> Stmt:
        if self.field in (object,):
            return Stmt(collected, {}, result=ListResult(DOCUMENT_RESULT), alias_to_result=alias_to_result,
                        aliases=self.aliases)

        if self.used_in_by:
            return Stmt(f'field_{self.field}', {}, result=VALUE_RESULT, alias_to_result=alias_to_result,
                        aliases=self.aliases)

        return Stmt(f'''{collected}[*].{self.field}''', {}, result=ListResult(VALUE_RESULT),
                    alias_to_result=alias_to_result,
                    aliases=self.aliases)

    def _to_select_stmt(self, prefix: str, relative_to: str, alias_to_result: Dict[str, Result]) -> Stmt:
        if self.field in (object,):
            return Stmt(relative_to, {}, result=DOCUMENT_RESULT, aliases=self.aliases, alias_to_result=alias_to_result)

        return Stmt(f'''{relative_to}.{self.field}''', {}, result=VALUE_RESULT, alias_to_result=alias_to_result,
                    aliases=self.aliases)

    def __str__(self) -> str:
        return str(self.field)
