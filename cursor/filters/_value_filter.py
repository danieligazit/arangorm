from dataclasses import dataclass
from typing import Any

from _stmt import Stmt
from cursor._operator import Operator


@dataclass
class ValueFilter():
    compare_to: Any
    operator: Operator
    outer_cursor: 'Cursor'

    def _to_filter_stmt(self, prefix: str, relative_to: str) -> Stmt:
        previous_stmt = self.outer_cursor._to_stmt(prefix, relative_to=relative_to)
        previous_str, bind_vars = previous_stmt.expand_without_return()

        if hasattr(self.compare_to, '_to_stmt'):
            compare_stmt = self.compare_to._to_stmt(prefix, relative_to=relative_to)
            compare_str, compare_bind_vars = compare_stmt.expand_without_return()
            bind_vars.update(compare_bind_vars)

            return Stmt(f'''
                LET {prefix}_sub = {previous_str}
                LET {prefix}_csub = {compare_str}
                FILTER {prefix}_sub {self.operator} {prefix}_csub''', bind_vars)

        bind_vars[f'{prefix}_compare'] = self.compare_to
        return Stmt(f'''
            LET {prefix}_sub = {previous_str}
            FILTER {prefix}_sub {self.operator} @{prefix}_compare
                ''', bind_vars)
