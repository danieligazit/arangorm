import json
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import repeat
from typing import Type, Any, Tuple, Dict, List, TypeVar

from cursor._aliased import Aliased
from cursor.filters._filters import eq

Q = TypeVar('Q', bound='Cursor')

DELIMITER = '\n'


@dataclass
class Cursor:
    project: Type['Document']
    db: Any
    matchers: List = field(default_factory=list, init=False)

    def all(self):
        query_stmt = self._to_stmt(prefix='p')
        query_str, bind_vars = query_stmt.expand()
        return map(self.project._load, self.db.db.aql.execute(query_str, bind_vars=bind_vars), repeat(self.db))

    def first(self):
        query_stmt = self._to_stmt(prefix='p')
        query_str, bind_vars = query_stmt.expand_without_return()

        project_stmt = self.project._get_stmt(prefix=f'project',
                                              max_recursion=defaultdict(lambda: 1, self.project._get_max_recursion()),
                                              relative_to=query_stmt.returns, parent=self)

        project_str, project_vars = project_stmt.expand()
        query_str += DELIMITER + 'RETURN ' + project_str

        # print(query_str)
        bind_vars.update(project_vars)
        # print(json.dumps(bind_vars))

        return next(map(self.project._load, self.db.db.aql.execute(query_str, bind_vars=bind_vars), repeat(self.db)),
                    None)

    def match(self, *match_objects, **key_value_match) -> Q:
        self.matchers += match_objects

        for key, value in key_value_match.items():
            self.matchers.append(eq(key, value))

        return self

    def _get_step_stmts(self, relative_to: str, prefix: str, returns: str, bind_vars: Dict[str, Any] = None,
                        bind_vars_index: int = 0) -> Tuple[str, Dict[str, Any], int]:
        step_stmts = []

        if not bind_vars:
            bind_vars = {}

        for matcher in self.matchers:
            query_stmt, matcher_vars = matcher._to_filter_stmt(prefix=f'{prefix}_{bind_vars_index}',
                                                               relative_to=relative_to).expand_without_return()
            step_stmts.append(query_stmt)
            bind_vars.update(matcher_vars)
            bind_vars_index += len(matcher_vars)

        # for alias in self.aliases:
        #     step_stmts.append(f'''LET {alias} = {returns}''')

        return DELIMITER.join(step_stmts), bind_vars, bind_vars_index
