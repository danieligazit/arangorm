from collections import defaultdict
from dataclasses import dataclass

from _stmt import Stmt
from cursor._cursor import Cursor


@dataclass
class DocumentCursor(Cursor):
    collection: str

    def _to_stmt(self, prefix: str):
        returns = f'o_{prefix}'
        step_str, bind_vars, _ = self._get_step_stmts(prefix=prefix, relative_to=returns,
                                                      returns=returns)

        return Stmt(f'''
            FOR {returns} IN {self.collection}
                {step_str}
            ''', returns=returns, bind_vars=bind_vars)