import json
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass, field
from inspect import isclass
from itertools import repeat
from typing import Type, Any, Tuple, Dict, List, TypeVar, Union, Optional

from _result import Result, DOCUMENT_RESULT, VALUE_RESULT, ListResult, DictResult
from _stmt import Stmt
from _utils import hasattrribute
from cursor._aliased import Aliased
from cursor._grouped import Grouped
from cursor._operator import Operator
from cursor._returns import Returns
from cursor._selected import Selected
from cursor._document_field import Field
from cursor._var import Var
from cursor.filters._attribute_filters import eq
from cursor.filters._value_filter import ValueFilter

Q = TypeVar('Q', bound='Cursor')

DELIMITER = '\n'


@dataclass
class Cursor(Returns, Aliased):
    project: Type['Document']
    db: Any
    matchers: List = field(default_factory=list, init=False)

    def all(self):
        query_stmt = self._to_stmt(prefix='p')
        query_str, bind_vars = query_stmt.expand()
        return map(self.project._load, self.db.db.aql.execute(query_str, bind_vars=bind_vars), repeat(self.db))

    def first(self):
        query_stmt = self._to_stmt(prefix='p')
        query_str, bind_vars = self.get_query()

        print(query_str)
        print(json.dumps(bind_vars))
        if self.project:
            loader = self.project._load
        else:
            loader = query_stmt.result._load

        try:
            result = self.db.db.aql.execute(query_str, bind_vars=bind_vars)
            return next(map(loader, result, repeat(self.db)))
        except Exception as e:
            print(e)

    def match(self, *match_objects, **key_value_match) -> Q:
        self.matchers += match_objects

        for key, value in key_value_match.items():
            self.matchers.append(eq(key, value))

        return self

    def get_query(self) -> Tuple[str, Dict[str, Any]]:
        query_stmt = self._to_stmt(prefix='p')

        if self.project:
            query_str, bind_vars = query_stmt.expand_without_return()
            project_stmt = self.project._get_stmt(prefix=f'project',
                                                  max_recursion=defaultdict(lambda: 1,
                                                                            self.project._get_max_recursion()),
                                                  relative_to=query_stmt.returns, parent=self)

            project_str, project_vars = project_stmt.expand()
            query_str += DELIMITER + 'RETURN ' + project_str
            bind_vars.update(project_vars)
            return query_str, bind_vars
        else:
            return query_stmt.expand()

    def _get_step_stmts(self, relative_to: str, prefix: str, returns: str, bind_vars: Dict[str, Any] = None,
                        bind_vars_index: int = 0) -> Tuple[str, Dict[str, Any], int]:
        step_stmts = []

        if not bind_vars:
            bind_vars = {}

        for matcher in self.matchers:

            if isinstance(matcher, InnerCursor):
                matcher.outer_cursor_returns = returns

            if isinstance(matcher, ValueFilter):
                traversal_cursor = matcher.outer_cursor.outer_cursor
                if traversal_cursor.__class__.__name__ == 'EdgeTraversalCursor':
                    traversal_cursor = traversal_cursor.outer_cursor
                if traversal_cursor.__class__.__name__ == 'EdgeTargetTraversalCursor':
                    while hasattrribute(traversal_cursor, 'outer_cursor'):
                        if hasattrribute(traversal_cursor.outer_cursor, 'outer_cursor') and traversal_cursor.outer_cursor.outer_cursor:
                            traversal_cursor = traversal_cursor.outer_cursor.outer_cursor
                            continue
                        break

                if traversal_cursor:
                    traversal_cursor.outer_cursor_returns = relative_to

            query_stmt, matcher_vars = matcher._to_filter_stmt(prefix=f'{prefix}_{bind_vars_index}',
                                                               relative_to=relative_to).expand_without_return()
            step_stmts.append(query_stmt)
            bind_vars.update(matcher_vars)
            bind_vars_index += len(matcher_vars)

        for alias in self.aliases:
            step_stmts.append(f'''LET {alias} = {returns}''')

        return DELIMITER.join(step_stmts), bind_vars, bind_vars_index

    def array(self, inner_query: 'InnerQuery') -> 'Array':
        return Array(outer_cursor=self, inner_cursor=inner_query, project=None, db=self.db)

    def group(self, *fields: Union[str, 'Field', Type['Document'], Type[object], 'Var'],
              **field_to_display_to_field: Union[str, 'Grouped', Type['Document'], Type[object], 'Var']) -> 'Group':

        if len(fields) == 0 and len(field_to_display_to_field) == 0:
            fields = [object]

        display_field_to_grouped = {}

        for field in fields:
            if isinstance(field, Field):
                display_field_to_grouped[field.field] = field
                continue

            if isinstance(field, Var):
                display_field_to_grouped[field._name] = field
                continue

            if isinstance(field, Grouped):
                display_field_to_grouped[str(field)] = field
                continue

            display_field_to_grouped['document' if isclass(field) else str(field)] = Field(field=field)

        for display_field, field in field_to_display_to_field.items():
            if isinstance(field, Grouped):
                display_field_to_grouped[display_field] = field
                continue

            display_field_to_grouped[display_field] = Field(field=field)

        return Group(cursor=self, display_field_to_grouped=display_field_to_grouped, project=None, db=self.db)

    def select(self, *fields: Union[str, 'Field', Type['Document'], Type[object], 'Var'],
               **field_to_display_to_field: Union[str, 'Selected', Type['Document'], Type[object]]) -> 'Select':

        if len(fields) == 0 and len(field_to_display_to_field) == 0:
            fields = [object]

        display_field_to_grouped = {}

        for field in fields:
            if isinstance(field, Field):
                display_field_to_grouped[field.field] = field
                continue

            if isinstance(field, Var):
                display_field_to_grouped[field._name] = field
                continue

            display_field_to_grouped['document' if isclass(field) else str(field)] = Field(field=field)

        for display_field, field in field_to_display_to_field.items():
            if isinstance(field, Selected):
                display_field_to_grouped[display_field] = field
                continue

            display_field_to_grouped[display_field] = Field(field=field)

        return Select(cursor=self, display_field_to_selected=display_field_to_grouped, project=None, db=self.db)

    def count(self):
        return CountCursor(outer_cursor=self, project=None, db=self.db, outer_cursor_returns='')


@dataclass
class Array(Cursor):
    outer_cursor: Cursor
    inner_cursor: Cursor

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

        self.inner_cursor.outer_cursor_returns = f'oqr_{prefix}'

        outer_stmt = self.outer_cursor._to_stmt(f'{prefix}_0', alias_to_result=alias_to_result)
        alias_to_result.update(outer_stmt.alias_to_result)
        outer_str, outer_bind_vars = outer_stmt.expand_without_return()
        bind_vars.update(outer_bind_vars)

        inner_stmt = self.inner_cursor._to_stmt(f'{prefix}_1', alias_to_result=alias_to_result)
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


@dataclass
class Group(Cursor):
    cursor: Cursor
    display_field_to_grouped: Dict[str, Grouped] = field(default_factory=dict)
    by_fields: List[str] = field(default_factory=list)

    def __post_init__(self):
        self.aliases = self.cursor.aliases

    def by(self, *fields: str) -> 'Group':
        for field in fields:
            self.by_fields.append(field)

        return self

    def _to_stmt(self, prefix: str = 'p', alias_to_result: Dict[str, Result] = None) -> Stmt:
        if not alias_to_result:
            alias_to_result = {}

        if len(self.by_fields) == 0:
            self.by_fields = ['_key']

        stmt = self.cursor._to_stmt(f'{prefix}_0', alias_to_result)

        alias_to_result.update(stmt.alias_to_result)
        previous, bind_vars = stmt.expand_without_return()
        previous_result = stmt.returns

        by_fields_stmt = []

        for by_field in self.by_fields:
            if isinstance(by_field, str):
                by_fields_stmt.append(f'field_{by_field} = {previous_result}.{by_field}')
            elif isinstance(by_field, Var):
                by_fields_stmt.append(f'field_{by_field._name} = {by_field.attribute_return}')
            else:
                raise TypeError

        bind_vars_index = 1
        groups_stmt = []

        result = {}
        for display_field, group_field in self.display_field_to_grouped.items():
            print(display_field, group_field)

            if isinstance(group_field, Field) and group_field.field in self.by_fields:
                group_field.used_in_by = True

            group_stmt = group_field._to_group_stmt(prefix=f'{prefix}_{bind_vars_index}', collected='groups',
                                                    alias_to_result=alias_to_result)
            alias_to_result.update(alias_to_result)
            group_str, b_vars = group_stmt.expand()
            result[display_field] = group_stmt.result

            field_bind = f'{prefix}_{bind_vars_index + 1}'
            groups_stmt.append(f'@{field_bind}: ({group_str})')
            bind_vars[field_bind] = display_field
            bind_vars.update(b_vars)
            bind_vars_index += 2

        return Stmt(f'''
        {previous}
        COLLECT {', '.join(by_fields_stmt)} INTO groups = {previous_result}
        RETURN {{
            {f',{DELIMITER}'.join(groups_stmt)}
        }}
        ''', bind_vars, result=self._get_result(DictResult(result)), aliases=self.aliases)


@dataclass
class Select(Cursor, Selected):
    cursor: Cursor
    display_field_to_selected: Dict[str, Selected] = field(default_factory=dict)
    selected_fields: List[str] = field(default_factory=list)

    def __post_init__(self):
        self.aliases = self.cursor.aliases

    def _to_stmt(self, prefix: str = 'p', alias_to_result: Dict[str, Result] = None) -> Stmt:
        if not alias_to_result:
            alias_to_result = {}

        stmt = self.cursor._to_stmt(prefix=f'{prefix}_0', alias_to_result=alias_to_result)
        alias_to_result.update(stmt.alias_to_result)
        previous, bind_vars = stmt.expand_without_return()

        bind_vars_index = 1
        groups_stmt = []

        result = {}
        for display_field, group_field in self.display_field_to_selected.items():
            if isinstance(group_field, Field) and group_field.field in self.selected_fields:
                group_field.used_in_by = True

            group_stmt = group_field._to_select_stmt(prefix=f'{prefix}_{bind_vars_index}', relative_to=stmt.returns,
                                                     alias_to_result=alias_to_result)
            alias_to_result.update(group_stmt.alias_to_result)
            group_str, b_vars = group_stmt.expand()

            result[display_field] = group_stmt.result

            field_bind = f'{prefix}_{bind_vars_index + 1}'
            groups_stmt.append(f'@{field_bind}: ({group_str})')
            bind_vars[field_bind] = display_field
            bind_vars.update(b_vars)
            bind_vars_index += 2

        return Stmt(f'''
        {previous}
        RETURN {{
            {f',{DELIMITER}'.join(groups_stmt)}
        }}
        ''', bind_vars, result=self._get_result(DictResult(result)), aliases=self.aliases,
                    alias_to_result=alias_to_result)

    def _to_select_stmt(self, prefix: str, relative_to: str, alias_to_result: Dict[str, Result] = None) -> Stmt:
        return self._to_stmt(prefix, alias_to_result=alias_to_result)


@dataclass
class InnerCursor(Cursor):
    outer_cursor_returns: Optional[str]


@dataclass
class ValueCursor(InnerCursor):
    outer_cursor: 'Cursor'

    def __gt__(self, other):
        return ValueFilter(outer_cursor=self, operator=Operator.GT, compare_to=other)

    def __ge__(self, other):
        return ValueFilter(outer_cursor=self, operator=Operator.GTE, compare_to=other)

    def __lt__(self, other):
        return ValueFilter(outer_cursor=self, operator=Operator.LT, compare_to=other)

    def __le__(self, other):
        return ValueFilter(outer_cursor=self, operator=Operator.LTE, compare_to=other)

    def __ne__(self, other):
        return ValueFilter(outer_cursor=self, operator=Operator.NE, compare_to=other)

    def __eq__(self, other):
        return ValueFilter(outer_cursor=self, operator=Operator.EQ, compare_to=other)


@dataclass(eq=False)
class CountCursor(ValueCursor):

    def _to_stmt(self, prefix: str, relative_to: str, alias_to_result: Dict[str, Any] = None) -> Stmt:
        if not alias_to_result:
            alias_to_result = {}

        previous_stmt = self.outer_cursor._get_traversal_stmt(prefix, relative_to=relative_to,
                                                              alias_to_result=alias_to_result)
        previous_str, previous_bind_vars = previous_stmt.expand_without_return()

        return Stmt(f'''LENGTH((
            {previous_str}
            RETURN 1
        ))''', bind_vars=previous_bind_vars)
