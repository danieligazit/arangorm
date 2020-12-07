from abc import ABC, abstractmethod
from dataclasses import dataclass, field as dataclass_field
from inspect import isclass
from typing import Any, List, Type, Dict, Tuple, TypeVar, Union

from collection import Collection, EdgeCollection
from result import ListResult, DictResult, AnyResult, Result, VALUE_RESULT, DOCUMENT_RESULT
from stmt import Stmt

DELIMITER = '\n'

Q = TypeVar('Q', bound='Query')
R = TypeVar('R', bound='Returns')


class Filter(ABC):
    @abstractmethod
    def _to_filter_stmt(self, prefix: str = 'p', relative_to: str = None) -> Stmt:
        pass


@dataclass
class Returns:
    attribute_return: str = dataclass_field(default='', init=False)
    attribute_return_list: List[str] = dataclass_field(default_factory=list, init=False)

    def _get_result(self, result: Result) -> Result:
        for attribute in self.attribute_return_list:
            result = result[attribute]

        return result

    def __getattr__(self, attr: str) -> R:
        self.attribute_return += '.' + attr
        self.attribute_return_list.append(attr)
        return self


@dataclass
class Aliased:
    aliases: List[str] = dataclass_field(default_factory=list, init=False)

    def as_var(self, variable: str) -> Q:
        self.aliases.append(variable)
        return self


@dataclass
class Query(ABC, Returns, Aliased):
    matchers: List[Filter] = dataclass_field(default_factory=list, init=False)

    @abstractmethod
    def _to_stmt(self, prefix: str = 'p', alias_to_result: Dict[str, Result] = None) -> Stmt:
        pass

    def match(self, *match_objects, **key_value_match) -> Q:
        self.matchers += match_objects

        for key, value in key_value_match.items():
            self.matchers.append(eq(key, value))

        return self

    def limit(self, limit) -> Q:
        # TODO
        raise ValueError

    def sort(self, fields: List[str]) -> Q:
        # TODO
        raise ValueError

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
                display_field_to_grouped[field.name] = field
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

        return Group(query=self, display_field_to_grouped=display_field_to_grouped)

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
                display_field_to_grouped[field.name] = field
                continue

            display_field_to_grouped[field.__name__ if isclass(field) else str(field)] = Field(field=field)

        for display_field, field in field_to_display_to_field.items():
            if isinstance(field, Selected):
                display_field_to_grouped[display_field] = field
                continue

            display_field_to_grouped[display_field] = Field(field=field)

        return Select(query=self, display_field_to_grouped=display_field_to_grouped)

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

        for alias in self.aliases:
            step_stmts.append(f'''LET {alias} = {returns}''')

        return DELIMITER.join(step_stmts), bind_vars, bind_vars_index

    def array(self, inner_query: 'InnerQuery') -> 'Array':
        return Array(outer_query=self, inner_query=inner_query)


def traversal_edge_collection_names(edge_collections: List[EdgeCollection]) -> str:
    return ",".join([e.name for e in edge_collections]) if edge_collections else ""


@dataclass
class Grouped(ABC):
    @abstractmethod
    def _to_group_stmt(self, prefix: str, collected, alias_to_result: Dict[str, Result]) -> Stmt:
        pass


@dataclass
class Selected(ABC):
    @abstractmethod
    def _to_select_stmt(self, prefix: str, relative_to: str, alias_to_result: Dict[str, Result]) -> Stmt:
        pass


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

@dataclass
class Group(Query):
    query: Query
    display_field_to_grouped: Dict[str, Grouped] = dataclass_field(default_factory=dict)
    by_fields: List[str] = dataclass_field(default_factory=list)

    def __post_init__(self):
        self.aliases = self.query.aliases

    def by(self, *fields: str) -> 'Group':
        for field in fields:
            self.by_fields.append(field)

        return self

    def _to_stmt(self, prefix: str = 'p', alias_to_result: Dict[str, Result] = None) -> Stmt:
        if not alias_to_result:
            alias_to_result = {}

        if len(self.by_fields) == 0:
            self.by_fields = ['_key']

        stmt = self.query._to_stmt(f'{prefix}_0', alias_to_result)

        alias_to_result.update(stmt.alias_to_result)
        previous, bind_vars = stmt.expand_without_return()
        previous_result = stmt.returns

        by_fields_stmt = []

        for by_field in self.by_fields:
            if isinstance(by_field, str):
                by_fields_stmt.append(f'field_{by_field} = {previous_result}.{by_field}')
            elif isinstance(by_field, Var):
                by_fields_stmt.append(f'field_{by_field.name} = {by_field.attribute_return}')
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
class Select(Query, Selected):
    query: Query
    display_field_to_grouped: Dict[str, Grouped] = dataclass_field(default_factory=dict)
    by_fields: List[str] = dataclass_field(default_factory=list)

    def __post_init__(self):
        self.aliases = self.query.aliases

    def _to_stmt(self, prefix: str = 'p', alias_to_result: Dict[str, Result] = None) -> Stmt:
        if not alias_to_result:
            alias_to_result = {}

        stmt = self.query._to_stmt(prefix=f'{prefix}_0', alias_to_result=alias_to_result)
        alias_to_result.update(stmt.alias_to_result)
        previous, bind_vars = stmt.expand_without_return()

        bind_vars_index = 1
        groups_stmt = []

        result = {}
        for display_field, group_field in self.display_field_to_grouped.items():
            if isinstance(group_field, Field) and group_field.field in self.by_fields:
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
class InnerQuery(Query):
    outer_query_returns: str

    def _set_outer_query(self, outer_query: Query):
        if not self.outer_query:
            self.outer_query = outer_query
            return

        if not isinstance(self.outer_query, InnerQuery):
            raise TypeError

        self.outer_query._set_outer_query(outer_query)


@dataclass
class EdgeQuery(Filter, Grouped, Selected, InnerQuery):
    edge_collections: List[EdgeCollection]
    outer_query: Query
    direction: str
    min_depth: int = dataclass_field(default=1)
    max_depth: int = dataclass_field(default=1)

    def __post_init__(self):
        if self.max_depth is None:
            if self.min_depth is None:
                self.min_depth, self.max_depth = 1, 1
                return
            self.max_depth = self.min_depth
            return

        if self.min_depth is None:
            self.min_depth = 1

    def to(self, *target_collection_types: Type['Document']):
        return EdgeTargetQuery(
            outer_query_returns='',
            outer_query=self,
            target_collections=[t.get_collection() for t in target_collection_types],
            direction=self.direction,
        )

    def _get_traversal_stmt(self, prefix: str, relative_to: str = '', alias_to_result: Dict[str, Result] = None):
        if not alias_to_result:
            alias_to_result = {}

        result = self._get_result(
            AnyResult([e.document_type for e in self.edge_collections])) if self.edge_collections else DOCUMENT_RESULT
        step_stmts, bind_vars, bind_vars_index = self._get_step_stmts(relative_to=f'{prefix}_e',
                                                                      returns=f'{prefix}_e' + self.attribute_return,
                                                                      prefix=prefix)

        if self.outer_query:
            previous_stmt = self.outer_query._to_stmt(prefix=f'{prefix}_0', alias_to_result=alias_to_result)
            alias_to_result.update(previous_stmt.alias_to_result)
            previous_str, previous_vars = previous_stmt.expand_without_return()
            bind_vars.update(previous_vars)

            return Stmt(f'''
                {previous_str}
                    FOR {prefix}_v, {prefix}_e IN {self.min_depth}..{self.max_depth} {self.direction} {previous_stmt.returns}._id {traversal_edge_collection_names(self.edge_collections)}
                        {step_stmts}
                ''', bind_vars, returns=f'{prefix}_e' + self.attribute_return, result=result, aliases=self.aliases,
                        alias_to_result=alias_to_result)

        return Stmt(f'''
            FOR {prefix}_v, {prefix}_e IN {self.min_depth}..{self.max_depth} {self.direction} {relative_to}._id {traversal_edge_collection_names(self.edge_collections)}
                {step_stmts}
        ''', bind_vars, returns=f'{prefix}_e' + self.attribute_return, result=result, aliases=self.aliases)

    def _to_stmt(self, prefix: str = 'p', alias_to_result: Dict[str, Result] = None) -> Stmt:
        return self._get_traversal_stmt(prefix, alias_to_result=alias_to_result, relative_to=self.outer_query_returns)

    def _to_filter_stmt(self, prefix: str = 'p', relative_to: str = None) -> Stmt:
        traversal_stmt = self._get_traversal_stmt(prefix, relative_to=relative_to)
        traversal_stmt.query_str = f'''
            LET {prefix}_sub = (
                {traversal_stmt.query_str}
                RETURN 1
            )
    
            FILTER LENGTH({prefix}_sub) > 0'''
        return traversal_stmt

    def _to_group_stmt(self, prefix: str, collected: str, alias_to_result: Dict[str, Result]) -> Stmt:
        traversal_stmt = self._get_traversal_stmt(prefix, relative_to=f'{prefix}_doc', alias_to_result=alias_to_result)

        traversal_stmt.query_str = f'''
            FOR {prefix}_doc in {collected}[*]
                {traversal_stmt.query_str}
        '''

        traversal_stmt.result = ListResult(AnyResult([e.document_type for e in self.edge_collections]))

        return traversal_stmt

    def _to_select_stmt(self, prefix: str, relative_to: str, alias_to_result: Dict[str, Result]) -> Stmt:
        traversal_stmt = self._get_traversal_stmt(prefix, relative_to=relative_to, alias_to_result=alias_to_result)
        traversal_stmt.result = AnyResult([e.document_type for e in self.edge_collections])
        return traversal_stmt


@dataclass
class DocumentQuery(Query, Selected):
    collection: Collection

    def out(self, *edge_collection_types: Type['Edge'], min_depth: int = None, max_depth: int = None) -> EdgeQuery:
        return EdgeQuery(
            outer_query_returns='',
            edge_collections=[e.get_collection() for e in edge_collection_types],
            direction='OUTBOUND',
            outer_query=self,
            min_depth=min_depth,
            max_depth=max_depth
        )

    def inbound(self, *edge_collection_types: Type['Edge'], min_depth: int = None, max_depth: int = None) -> EdgeQuery:
        return EdgeQuery(
            outer_query_returns='',
            edge_collections=[e.get_collection() for e in edge_collection_types],
            direction='INBOUND',
            outer_query=self,
            min_depth=min_depth,
            max_depth=max_depth
        )

    def connected_by(self, *edge_collection_types: Type['Edge'], min_depth: int = None, max_depth: int = None):
        return EdgeQuery(
            outer_query_returns='',
            edge_collections=[e.get_collection() for e in edge_collection_types],
            direction='ANY',
            outer_query=self,
            min_depth=min_depth,
            max_depth=max_depth
        )

    def _to_stmt(self, prefix: str = 'p', alias_to_result: Dict[str, Result] = None) -> Stmt:
        if not alias_to_result:
            alias_to_result = {}
        returns = f'o_{prefix}' + self.attribute_return
        step_stmts, bind_vars, bind_vars_index = self._get_step_stmts(returns=returns, relative_to=f'o_{prefix}',
                                                                      prefix=prefix, bind_vars_index=1)

        if self.collection:
            return Stmt(f'''
            FOR o_{prefix} IN {self.collection.name}
                {step_stmts}
            ''', bind_vars, returns=returns, result=self._get_result(self.collection.document_type),
                        aliases=self.aliases, alias_to_result=alias_to_result)

        if self.previous:
            previous_stmt = self.previous._to_stmt(prefix=f'{prefix}_0', alias_to_result=alias_to_result)
            alias_to_result.update(previous_stmt.alias_to_result)
            previous_str, previous_bind_vars = previous_stmt.expand()
            return Stmt(f'''
            LET previous = ({previous_str})
                FOR o_{prefix} IN previous
                    {step_stmts}
            ''', bind_vars, returns=returns, result=self._get_result(self.collection.document_type),
                        aliases=self.aliases)

        raise ValueError

    def _to_select_stmt(self, prefix: str, relative_to: str, alias_to_result: Dict[str, Result]) -> Stmt:

        returns = f'o_{prefix}' + self.attribute_return
        step_stmts, bind_vars, bind_vars_index = self._get_step_stmts(returns=returns, relative_to=f'o_{prefix}',
                                                                      prefix=prefix, bind_vars_index=1)

        if self.collection:
            return Stmt(f'''
                    FOR o_{prefix} IN {self.collection.name}
                        {step_stmts}
                    ''', bind_vars, returns=f'o_{prefix}' + self.attribute_return,
                        result=VALUE_RESULT if self.attribute_return else self.collection.document_type,
                        aliases=self.aliases)

        if self.previous:
            previous_stmt = self.previous._to_stmt(prefix=f'{prefix}_0', alias_to_result=alias_to_result)
            alias_to_result.update(previous_stmt.alias_to_result)
            previous_str, previous_bind_vars = previous_stmt.expand()
            bind_vars.update(previous_bind_vars)
            return Stmt(f'''
                    LET previous = ({previous_str})
                        FOR o_{prefix} IN previous
                        {step_stmts}
                    ''', bind_vars, returns=f'o_{prefix}{self.attribute_return}',
                        result=VALUE_RESULT if self.attribute_return else self.collection.document_type,
                        aliases=self.aliases)

        raise ValueError


@dataclass
class EdgeTargetQuery(Filter, Grouped, InnerQuery):
    target_collections: List[Collection]
    outer_query: Union[Query, None]
    direction: str

    def out(self, *edge_collection_types: Type['Edge'], min_depth: int = None, max_depth: int = None) -> EdgeQuery:
        return EdgeQuery(
            outer_query_returns='',
            edge_collections=[e.get_collection() for e in edge_collection_types],
            direction='OUTBOUND',
            outer_query=self,
            min_depth=min_depth,
            max_depth=max_depth
        )

    def inbound(self, *edge_collection_types: Type['Edge'], min_depth: int = None, max_depth: int = None) -> EdgeQuery:
        return EdgeQuery(
            outer_query_returns='',
            edge_collections=[e.get_collection() for e in edge_collection_types],
            direction='INBOUND',
            outer_query=self,
            min_depth=min_depth,
            max_depth=max_depth
        )

    def connected_by(self, *edge_collection_types: Type['Edge'], min_depth: int = None, max_depth: int = None):
        return EdgeQuery(
            outer_query_returns='',
            edge_collections=[e.get_collection() for e in edge_collection_types],
            direction='ANY',
            outer_query=self,
            min_depth=min_depth,
            max_depth=max_depth
        )

    def _to_stmt(self, prefix: str = 'p', alias_to_result: Dict[str, Result] = None) -> Stmt:
        return self._get_traversal_stmt(prefix, f'{prefix}_v', alias_to_result)

    def _get_traversal_stmt(self, prefix: str, relative_to: str, alias_to_result: Dict[str, Result] = None):
        if not alias_to_result:
            alias_to_result = {}

        returns = f'{prefix}_v'
        result = self._get_result(AnyResult([t.document_type for t in self.target_collections])) if len(
            self.target_collections) > 0 else DOCUMENT_RESULT

        step_stmts, bind_vars, bind_vars_index = self._get_step_stmts(relative_to=returns,
                                                                      returns=returns + self.attribute_return,
                                                                      prefix=prefix, bind_vars_index=1)

        filter_target_collection = f'''FILTER {" OR ".join([f"IS_SAME_COLLECTION('{t.name}', {returns})" for t in self.target_collections])}''' if self.target_collections else ''

        if self.outer_query:

            edge_query = self.outer_query

            previous_str = ''

            outer_query_step_stmts, bind_vars, bind_vars_index = self.outer_query._get_step_stmts(
                relative_to=f'{prefix}_e', bind_vars=bind_vars, bind_vars_index=bind_vars_index,
                prefix=f'{prefix}', returns=f'{prefix}_e' + self.outer_query.attribute_return)

            if self.outer_query.outer_query:
                previous_stmt = self.outer_query.outer_query._to_stmt(f'{prefix}_0', alias_to_result=alias_to_result)
                alias_to_result.update(previous_stmt.alias_to_result)
                previous_str, previous_bind_vars = previous_stmt.expand_without_return()
                bind_vars.update(previous_bind_vars)
                relative_to = previous_stmt.returns

            return Stmt(f'''
            {previous_str}
            FOR {prefix}_v, {prefix}_e IN {self.outer_query.min_depth}..{self.outer_query.max_depth} {edge_query.direction} {relative_to}._id {",".join([e.name for e in edge_query.edge_collections]) if edge_query.edge_collections else ""}
                {filter_target_collection}
                {step_stmts}
                {outer_query_step_stmts}
            ''', bind_vars, alias_to_result=alias_to_result, returns=returns + self.attribute_return,
                        result=result, aliases=self.aliases)

        return Stmt(f'''
            {filter_target_collection}
            {step_stmts}
        ''', bind_vars, alias_to_result=alias_to_result, result=result,
                    returns=returns + self.attribute_return, aliases=self.aliases)

    def _to_filter_stmt(self, prefix: str = 'p', relative_to: str = None) -> Stmt:

        relative_to = relative_to[:-1] + 'v' if relative_to.endswith('e') else relative_to
        returns = f'{prefix}_v'

        if self.outer_query:
            edge_query = self.outer_query
            filter_target_collection = f'''FILTER {" OR ".join([f"IS_SAME_COLLECTION('{t.name}', {returns})" for t in self.target_collections])}''' if self.target_collections else ''
            previous_str = ''

            if self.outer_query.outer_query:
                filter_target_collection = f'''FILTER {" OR ".join([f"IS_SAME_COLLECTION('{t.name}', {relative_to})" for t in self.target_collections])}''' if self.target_collections else ''

                step_stmts, bind_vars, bind_vars_index = self._get_step_stmts(relative_to=relative_to,
                                                                              returns=returns + self.attribute_return,
                                                                              prefix=prefix, bind_vars_index=0)

                previous_stmt = self.outer_query.outer_query._to_stmt(f'{prefix}_0')
                previous_str, previous_bind_vars = previous_stmt.expand_without_return()
                bind_vars.update(previous_bind_vars)
                relative_to = previous_stmt.returns

            else:
                step_stmts, bind_vars, bind_vars_index = self._get_step_stmts(relative_to=returns,
                                                                              returns=returns + self.attribute_return,
                                                                              prefix=prefix, bind_vars_index=0)

            outer_query_step_stmts, bind_vars, bind_vars_index = self.outer_query._get_step_stmts(
                relative_to=f'{prefix}_e', bind_vars=bind_vars, bind_vars_index=bind_vars_index,
                prefix=f'{prefix}', returns=f'{prefix}_e' + self.outer_query.attribute_return)

            return Stmt(f'''
            LET {prefix}_sub = (
                {previous_str}
                FOR {prefix}_v, {prefix}_e IN {self.outer_query.min_depth}..{self.outer_query.max_depth} {edge_query.direction} {relative_to}._id {",".join([e.name for e in edge_query.edge_collections]) if edge_query.edge_collections else ""}
                    {filter_target_collection}
                    {step_stmts}
                    {outer_query_step_stmts}
                    RETURN 1
            )
            FILTER LENGTH({prefix}_sub) > 0
            ''', bind_vars)

        filter_target_collection = f'''FILTER {" OR ".join([f"IS_SAME_COLLECTION('{t.name}', {relative_to})" for t in self.target_collections])}''' if self.target_collections else ''

        step_stmts, bind_vars, bind_vars_index = self._get_step_stmts(relative_to=relative_to,
                                                                      returns=returns + self.attribute_return,
                                                                      prefix=prefix, bind_vars_index=0)

        return Stmt(f'''
            LET {prefix}_sub = (
                {filter_target_collection}
                {step_stmts}
                RETURN 1
            )
            FILTER LENGTH({prefix}_sub) > 0
            
        ''', bind_vars)

    def _to_group_stmt(self, prefix: str, collected, alias_to_result: Dict[str, Result]) -> Stmt:
        traversal_stmt = self._get_traversal_stmt(prefix, relative_to=f'{prefix}_doc', alias_to_result=alias_to_result)

        traversal_stmt.query_str = f'''
                    FOR {prefix}_doc in {collected}[*]
                        {traversal_stmt.query_str}
                '''

        traversal_stmt.result = ListResult(AnyResult([e.document_type for e in self.target_collections]))

        return traversal_stmt


class AttributeFilter(Filter):
    attribute: str
    operator: str
    compare_value: Any

    def __init__(self, attribute: str, operator: str, compare_value: Any):
        super().__init__()
        self.attribute = attribute
        self.operator = operator
        self.compare_value = compare_value

    def _to_filter_stmt(self, prefix: str = 'p', relative_to: str = None) -> Stmt:
        if isinstance(self.compare_value, Var):
            return Stmt(f'FILTER {relative_to}.{self.attribute} {self.operator} {self.compare_value.attribute_return}',
                        {})
        return Stmt(f'FILTER {relative_to}.{self.attribute} {self.operator} @{prefix} ', {prefix: self.compare_value})


@dataclass
class Array(Query):
    outer_query: Query
    inner_query: InnerQuery

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


def like(attribute: str, compare_value: Any) -> AttributeFilter:
    return AttributeFilter(attribute=attribute, operator='LIKE', compare_value=compare_value)


def eq(attribute: str, compare_value: Any) -> AttributeFilter:
    return AttributeFilter(attribute=attribute, operator='==', compare_value=compare_value)


def gt(attribute: str, compare_value: Any) -> AttributeFilter:
    return AttributeFilter(attribute=attribute, operator='>', compare_value=compare_value)


def out(*edge_collection_types: Type['Edge'], min_depth: int = None, max_depth: int = None) -> EdgeQuery:
    return EdgeQuery(
        outer_query_returns='',
        edge_collections=[e.get_collection() for e in edge_collection_types],
        direction='OUTBOUND',
        outer_query=None,
        min_depth=min_depth,
        max_depth=max_depth
    )


def inbound(*edge_collection_types: Type['Edge'], min_depth: int = None, max_depth: int = None) -> EdgeQuery:
    return EdgeQuery(
        outer_query_returns='',
        edge_collections=[e.get_collection() for e in edge_collection_types],
        direction='INBOUND',
        outer_query=None,
        min_depth=min_depth,
        max_depth=max_depth
    )


def connected_by(*edge_collection_types: Type['Edge'], min_depth: int = None, max_depth: int = None) -> EdgeQuery:
    return EdgeQuery(
        outer_query_returns='',
        edge_collections=[e.get_collection() for e in edge_collection_types],
        direction='ANY',
        outer_query=None,
        min_depth=min_depth,
        max_depth=max_depth
    )


def to(*collection_types: Type['Document']) -> EdgeTargetQuery:
    return EdgeTargetQuery(
        target_collections=[c.get_collection() for c in collection_types],
        outer_query=None,
        outer_query_returns='',
        direction='OUTBOUND'
    )


def max(field: Union[str, Grouped, Selected]):
    if isinstance(field, str):
        return Aggregated(group=Field(field=field), func='MAX')

    return Aggregated(group=field, func='MAX')


def count(field: Union[str, Grouped]):
    if isinstance(field, str):
        return Aggregated(group=Field(field=field), func='COUNT')

    return Aggregated(group=field, func='COUNT')


@dataclass
class Var(Selected, Returns, Grouped):
    name: str

    def _to_group_stmt(self, prefix: str, alias_to_result: Dict[str, Result], collected: str = 'groups') -> Stmt:
        return Stmt(f'''@{prefix}_0''', {f'{prefix}_0': self.attribute_return}, result=alias_to_result[self.name])

    def _to_select_stmt(self, prefix: str, alias_to_result: Dict[str, Result], relative_to: str = '') -> Stmt:
        return Stmt(f'''@{prefix}_0''', {f'{prefix}_0': self.attribute_return}, result=alias_to_result[self.name])


def var(expression: str):
    v = Var(name=expression)
    return v

# def main():
#     query = Company.match().as_var('a').match(some='value').out(LocatedIn).to(Country) \
#         .array(
#         out(LocatedIn).to(Country).array(
#             out(LocatedIn).to(Country)
#         )
#     ).as_var('b')
#
#     client = ArangoClient()
#     db = client.db('test', username='root', password='')
#
#     query_stmt = query._to_stmt()
#     query_str, bind_vars = query_stmt.expand()
#     print(query_stmt.result)
#     print(query_str, bind_vars)
#     # result = db.aql.execute(query_str, bind_vars=bind_vars)
#     # print(json.dumps(list(result), indent=4))
#
#
# if __name__ == '__main__':
#     main()
