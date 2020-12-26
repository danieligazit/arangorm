from collections import defaultdict
from dataclasses import dataclass

from _direction import Direction
from _stmt import Stmt
from cursor._cursor import Cursor
from cursor._document_cursor import DocumentCursor


@dataclass
class EdgeCursor(Cursor):
    collection: str

    def _to_stmt(self, prefix: str):
        edge_var = f'e_{prefix}'
        step_str, bind_vars, _ = self._get_step_stmts(prefix=prefix, relative_to=edge_var,
                                                      returns=edge_var)

        return Stmt(f'''
            FOR {edge_var} IN {self.collection}
                {step_str}
            ''', returns=edge_var, bind_vars=bind_vars)
