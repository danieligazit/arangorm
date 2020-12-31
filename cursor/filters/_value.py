from _stmt import Stmt


class ValueFilter:
    previous_cursor: 'Cursor'

    def _to_filter_stmt(self, prefix: str, relative_to: str) -> Stmt:
        return Stmt(f'FILTER {relative_to}.{self.attribute} {self.operator} @{prefix} ', {prefix: self.compare_value})
