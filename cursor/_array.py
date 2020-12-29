from dataclasses import dataclass
from typing import Dict

from _result import Result, ListResult
from _stmt import Stmt
from cursor._cursor import Cursor


@dataclass
class Array(Cursor):
    outer_cursor: Cursor
    inner_query: Cursor

    def __getitem__(self, item) -> 'Array':
        selection = f'[{item}]'
        self.attribute_return += selection
        self.attribute_return_list.append(selection)
        return self

    def _to_stmt(self, prefix: str = 'p', alias_to_result: Dict[str, Result] = None) -> Stmt:
        if not alias_to_result:
            alias_to_result = {}

        returns = f'array_{prefix}'
        step_stmts, bind_vars, bind_vars_index = self._get_step_stmts(relative_to=returns,
                                                                      returns=f'array_{prefix}' + self.attribute_return,
                                                                      prefix=prefix, bind_vars_index=1)

        self.inner_query.outer_query_returns = f'oqr_{prefix}'

        outer_stmt = self.outer_query._to_stmt(f'{prefix}_0', alias_to_result=alias_to_result)
        alias_to_result.update(outer_stmt.alias_to_result)
        outer_str, outer_bind_vars = outer_stmt.expand_without_return()
        bind_vars.update(outer_bind_vars)

        inner_stmt = self.inner_query._to_stmt(f'{prefix}_1', alias_to_result=alias_to_result)
        alias_to_result.update(inner_stmt.alias_to_result)
        inner_str, inner_bind_vars = inner_stmt.expand()
        bind_vars.update(inner_bind_vars)

        return Stmt(f'''
            {outer_str}
            LET oqr_{prefix} = {outer_stmt.returns}
            LET {returns} = (
                {inner_str}
            )
            {step_stmts}
        ''', bind_vars=bind_vars, returns=returns + self.attribute_return, alias_to_result=alias_to_result,
                    aliases=self.aliases,
                    result=self._get_result(ListResult(inner_stmt.result)))