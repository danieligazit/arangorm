import json
from abc import ABC, abstractmethod
from datetime import datetime
from dataclasses import dataclass, field as dataclass_field
from inspect import isclass
from typing import Any, List, Type, Dict, Tuple, TypeVar, Union

from arango import ArangoClient

from document import Edge, Document
from collection import Collection, EdgeCollection
import model.collection_definition as col

DELIMITER = '\n'

Q = TypeVar('Q', bound='Query')
R = TypeVar('R', bound='Returns')


@dataclass
class Result:
    pass


@dataclass
class ValueResult(Result):
    pass


@dataclass
class ListResult(Result):
    inner_result: Result


@dataclass
class AnyResult(Result):
    inner_result: List[Result]


VALUE = ValueResult()


@dataclass
class Stmt:
    query_str: str
    bind_vars: Dict[str, Any]
    aliases: List[str] = dataclass_field(default_factory=list)
    returns: str = dataclass_field(default=None)
    result: Any = dataclass_field(default=None)
    alias_to_result: Dict[str, Result] = dataclass_field(default=None)

    def __post_init__(self):
        self.alias_to_result = {alias: self.result for alias in self.aliases}

    def expand(self) -> Tuple[str, Dict[str, Any]]:
        if not self.returns:
            return self.expand_without_return()
        return self.query_str + f' RETURN {self.returns}', self.bind_vars

    def expand_without_return(self) -> Tuple[str, Dict[str, Any]]:
        return self.query_str, self.bind_vars


class Filter(ABC):
    @abstractmethod
    def _to_filter_stmt(self, prefix: str = 'p', relative_to: str = None) -> Stmt:
        pass


@dataclass
class Returns:
    attribute_return: str

    def __getattr__(self, attr: str) -> R:
        self.attribute_return = '.' + attr
        return self

@dataclass
class Aliased:
    aliases: List[str]

    def as_var(self, variable: str) -> Q:
        self.aliases.append(variable)
        return self


@dataclass
class Query(ABC, Returns, Aliased):
    matchers: List[Filter]

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

    def group(self, *fields: Union[str, 'Field', Type[Document], Type[object]],
              **field_to_display_to_field: Union[str, 'Grouped', Type[Document], Type[object]]) -> 'Group':

        if len(fields) == 0 and len(field_to_display_to_field) == 0:
            fields = [object]

        display_field_to_grouped = {}

        for field in fields:
            if isinstance(field, Field):
                display_field_to_grouped[field.field] = field
                continue

            display_field_to_grouped[field.__name__ if isclass(field) else str(field)] = Field(field=field)

        for display_field, field in field_to_display_to_field.items():
            if isinstance(field, Grouped):
                display_field_to_grouped[display_field] = field
                continue

            display_field_to_grouped[display_field] = Field(field=field)

        return Group(query=self, display_field_to_grouped=display_field_to_grouped, matchers=[])

    def select(self, *fields: Union[str, 'Field', Type[Document], Type[object], 'Var'],
               **field_to_display_to_field: Union[str, 'Selected', Type[Document], Type[object]]) -> 'Select':

        if len(fields) == 0 and len(field_to_display_to_field) == 0:
            fields = [object]

        display_field_to_grouped = {}

        for field in fields:
            if isinstance(field, Field):
                display_field_to_grouped[field.field] = field
                continue

            if isinstance(field, Var):
                display_field_to_grouped[field.name] = field

            display_field_to_grouped[field.__name__ if isclass(field) else str(field)] = Field(field=field)

        for display_field, field in field_to_display_to_field.items():
            if isinstance(field, Selected):
                display_field_to_grouped[display_field] = field
                continue

            display_field_to_grouped[display_field] = Field(field=field)

        return Select(query=self, display_field_to_grouped=display_field_to_grouped, matchers=[], aliases=[],
                      attribute_return='')

    def _get_step_stmts(self, relative_to: str, prefix: str, returns: str, bind_vars: Dict[str, Any] = None,
                        bind_vars_index: int = 0) -> Tuple[str, Dict[str, Any], int]:
        step_stmts = []

        if not bind_vars:
            bind_vars = {}

        for matcher in self.matchers:
            query_stmt, matcher_vars = matcher._to_filter_stmt(prefix=f'{prefix}_{bind_vars_index}',
                                                               relative_to=relative_to).expand()
            step_stmts.append(query_stmt)
            bind_vars.update(matcher_vars)
            bind_vars_index += len(matcher_vars)

        for alias in self.aliases:
            step_stmts.append(f'''LET {alias} = {returns}''')

        return DELIMITER.join(step_stmts), bind_vars, bind_vars_index


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
class Aggregated(Grouped, Selected):
    group: Union[Grouped, Selected]
    func: str

    def _to_group_stmt(self, prefix: str, collected: str, alias_to_result: Dict[str, Result]) -> Stmt:
        stmt = self.group._to_group_stmt(prefix, collected=collected, alias_to_result=alias_to_result)
        query_str, bind_vars = stmt.expand()

        return Stmt(f'''{self.func}({query_str})''', bind_vars, result=VALUE)

    def _to_select_stmt(self, prefix: str, relative_to: str, alias_to_result: Dict[str, Result]) -> Stmt:
        stmt = self.group._to_select_stmt(prefix, relative_to=relative_to, alias_to_result=alias_to_result)
        query_str, bind_vars = stmt.expand()

        return Stmt(f'''{self.func}({query_str})''', bind_vars, result=VALUE)


@dataclass
class Field(Grouped, Selected):
    field: Union[str, Type[Document], Type[object]]
    used_in_by: bool = dataclass_field(default=False)

    def _to_group_stmt(self, prefix: str, collected: str, alias_to_result: Dict[str, Result]) -> Stmt:
        if self.field in (Document, object):
            return Stmt(collected, {}, result=ListResult(Document))

        if self.used_in_by:
            return Stmt(f'field_{self.field}', {}, result=VALUE)

        return Stmt(f'''{collected}[*].{self.field}''', {}, result=ListResult(VALUE))

    def _to_select_stmt(self, prefix: str, relative_to: str, alias_to_result: Dict[str, Result]) -> Stmt:
        if self.field in (Document, object):
            return Stmt(relative_to, {}, result=Document)

        return Stmt(f'''{relative_to}.{self.field}''', {}, result=VALUE)


@dataclass
class Group(Query):
    query: Query
    display_field_to_grouped: Dict[str, Grouped] = dataclass_field(default_factory=dict)
    by_fields: List[str] = dataclass_field(default_factory=list)

    def by(self, *fields: str) -> 'Group':
        self.by_fields = list(fields)
        return self

    def _to_stmt(self, prefix: str = 'p', alias_to_result: Dict[str, Result] = None) -> Stmt:
        if len(self.by_fields) == 0:
            self.by_fields = ['_key']

        stmt = self.query._to_stmt(f'{prefix}_0', alias_to_result)

        alias_to_result.update(stmt.alias_to_result)
        previous, bind_vars = stmt.expand_without_return()
        previous_result = stmt.returns

        by_fields_stmt = []

        for by_field in self.by_fields:
            by_fields_stmt.append(f'field_{by_field} = {previous_result}.{by_field}')

        bind_vars_index = 1
        groups_stmt = []

        result = {}
        for display_field, group_field in self.display_field_to_grouped.items():
            if isinstance(group_field, Field) and group_field.field in self.by_fields:
                group_field.used_in_by = True

            group_stmt = group_field._to_group_stmt(f'{prefix}_{bind_vars_index}', 'groups',
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
        ''', bind_vars, result=result, aliases=self.aliases)


@dataclass
class Select(Query, Selected):
    query: Query
    display_field_to_grouped: Dict[str, Grouped] = dataclass_field(default_factory=dict)
    by_fields: List[str] = dataclass_field(default_factory=list)

    def _to_stmt(self, prefix: str = 'p', alias_to_result: Dict[str, Result] = None) -> Stmt:
        if not alias_to_result:
            alias_to_result = {}

        stmt = self.query._to_stmt(f'{prefix}_0')
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
        RETURN {{
            {f',{DELIMITER}'.join(groups_stmt)}
        }}
        ''', bind_vars, result=result, aliases=self.aliases)

    def _to_select_stmt(self, prefix: str, relative_to: str, alias_to_result: Dict[str, Result] = None) -> Stmt:
        return self._to_stmt(prefix, alias_to_result=alias_to_result)


@dataclass
class EdgeQuery(Query, Filter, Grouped, Selected):
    edge_collections: List[EdgeCollection]
    direction: str
    previous_query: Query

    def to(self, *target_collection_types: Type[Document]):
        return EdgeTargetQuery(
            edge_query=self,
            matchers=[],
            target_collections=[t.get_collection() for t in target_collection_types]
        )

    def _get_traversal_stmt(self, prefix: str, relative_to: str = 'v', alias_to_result: Dict[str, Result] = None):
        result = AnyResult([e.document_type for e in self.edge_collections])
        step_stmts, bind_vars, bind_vars_index = self._get_step_stmts(relative_to=relative_to,
                                                                      returns='e' + self.attribute_return,
                                                                      prefix=prefix)

        if self.previous_query:
            previous_stmt = self.previous_query._to_stmt(prefix=f'{prefix}_0', alias_to_result=alias_to_result)
            previous_str, previous_vars = previous_stmt.expand()
            bind_vars.update(previous_vars)
            return Stmt(f'''
                {previous_str}
                    FOR v, e IN 1..1 {self.direction} {previous_stmt.returns}._id {traversal_edge_collection_names(self.edge_collections)}
                        {step_stmts}
                ''', bind_vars, returns='e' + self.attribute_return, result=result, aliases=self.aliases)

        return Stmt(f'''
            FOR v, e IN 1..1 {self.direction} {relative_to}._id {traversal_edge_collection_names(self.edge_collections)}
                {step_stmts}
        ''', bind_vars, returns='e' + self.attribute_return, result=result, aliases=self.aliases)

    def _to_stmt(self, prefix: str = 'p', alias_to_result: Dict[str, Result] = None) -> Stmt:
        return self._get_traversal_stmt(prefix, alias_to_result=alias_to_result)

    def _to_filter_stmt(self, prefix: str = 'p', relative_to: str = None) -> Stmt:
        traversal_stmt = self._get_traversal_stmt(prefix)

        traversal_stmt.query_str = f'''
            let sub = (
                {traversal_stmt.query_str}
            )
    
            FILTER LENGTH(sub) > 0'''
        return traversal_stmt

    def _to_group_stmt(self, prefix: str, collected: str, alias_to_result: Dict[str, Result]) -> Stmt:
        traversal_stmt = self._get_traversal_stmt(prefix, relative_to='doc', alias_to_result=alias_to_result)

        traversal_stmt.query_str = f'''
            FOR doc in {collected}[*]
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
    key_value_match: Dict[str, Any]

    def __post_init__(self):
        for key, value in self.key_value_match.items():
            self.matchers.append(eq(key, value))

    def out(self, *edge_collection_types: Type[Edge]) -> EdgeQuery:
        return EdgeQuery(
            edge_collections=[e.get_collection() for e in edge_collection_types],
            matchers=[],
            attribute_return='',
            aliases=[],
            direction='OUTBOUND',
            previous_query=self
        )

    def inbound(self, *edge_collection_types: Type[Edge]) -> EdgeQuery:
        return EdgeQuery(
            edge_collections=[e.get_collection() for e in edge_collection_types],
            matchers=[],
            attribute_return='',
            aliases=[],
            direction='INBOUND',
            previous_query=self
        )

    def _to_stmt(self, prefix: str = 'p', alias_to_result: Dict[str, Result] = None) -> Stmt:
        returns = f'o_{prefix}' + self.attribute_return
        step_stmts, bind_vars, bind_vars_index = self._get_step_stmts(returns=returns, relative_to=f'o_{prefix}',
                                                                      prefix=prefix, bind_vars_index=1)

        if self.collection:
            return Stmt(f'''
            FOR o_{prefix} IN {self.collection.name}
                {step_stmts}
            ''', bind_vars, returns=returns, result=VALUE if self.attribute_return else self.collection.document_type)

        if self.previous:
            previous_stmt, previous_bind_vars = self.previous._to_stmt(prefix=prefix, alias_to_result=alias_to_result).expand()
            bind_vars.update(previous_bind_vars)
            return Stmt(f'''
            LET previous = ({previous_stmt})
                FOR o_{prefix} IN previous
                    {step_stmts}
            ''', bind_vars, returns=returns, result=VALUE if self.attribute_return else self.collection.document_type)

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
                        result=VALUE if self.attribute_return else self.collection.document_type)

        if self.previous:
            previous_stmt, previous_bind_vars = self.previous._to_stmt(prefix=f'{prefix}_0', alias_to_result=alias_to_result).expand()
            bind_vars.update(previous_bind_vars)
            return Stmt(f'''
                    LET previous = ({previous_stmt})
                        FOR o_{prefix} IN previous
                        {step_stmts}
                    ''', bind_vars, returns=VALUE if self.attribute_return else self.collection.document_type)

        raise ValueError


@dataclass
class EdgeTargetQuery(DocumentQuery):
    target_collections: List[Collection]
    edge_query: 'EdgeQuery'

    def _to_stmt(self, prefix: str = 'p', alias_to_result: Dict[str, Result] = None) -> Stmt:
        if not alias_to_result:
            alias_to_result = {}

        if not self.edge_query.previous_query:
            raise ValueError

        previous_stmt = self.edge_query.previous_query._to_stmt(prefix=f'{prefix}_0', alias_to_result=alias_to_result)
        previous_str, vars = previous_stmt.expand_without_return()
        traversal_stmt = self._get_traversal_stmt(prefix=prefix, relative_to=previous_stmt.returns, index=1)
        traversal_str, bind_vars = traversal_stmt.expand_without_return()

        bind_vars.update(vars)

        return Stmt(f'''
        {previous_str}
            {traversal_str}''', bind_vars, returns=traversal_stmt.returns + self.attribute_return)

    def _get_traversal_stmt(self, prefix: str = 'p', relative_to: str = None, index: int = 0) -> Stmt:

        bind_vars, filter_stmts, bind_vars_index = {}, [], index

        for matcher in self.edge_query.matchers:
            query_stmt, vars = matcher._to_filter_stmt(prefix=f'{prefix}_{bind_vars_index}', relative_to='e').expand()
            filter_stmts.append(query_stmt)
            bind_vars.update(vars)
            bind_vars_index += 1

        for matcher in self.matchers:
            query_stmt, vars = matcher._to_filter_stmt(prefix=f'{prefix}_{bind_vars_index}', relative_to='v').expand()
            filter_stmts.append(query_stmt)
            bind_vars.update(vars)
            bind_vars_index += 1

        return Stmt(f'''
        FOR v, e IN 1..1 {self.edge_query.direction} {relative_to}._id {",".join([e.name for e in self.edge_query.edge_collections]) if self.edge_query.edge_collections else ""}
            filter {" or ".join([f"IS_SAME_COLLECTION('{t.name}', v)" for t in self.target_collections])}
            {DELIMITER.join(filter_stmts)}
        ''', bind_vars, returns='v' + self.attribute_return)

    def _to_filter_stmt(self, prefix: str = 'p', relative_to: str = None) -> Stmt:
        traversal_stmt, bind_vars = self._get_traversal_stmt(prefix=prefix, relative_to=relative_to)

        return Stmt(f'''
        LET sub = (
            {traversal_stmt}
            RETURN 1
        )

        FILTER LENGTH(sub) > 0
        ''', bind_vars)


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
            return Stmt(f'filter {relative_to}.{self.attribute} {self.operator} {self.compare_value.expression}', {})
        return Stmt(f'filter {relative_to}.{self.attribute} {self.operator} @{prefix} ', {prefix: self.compare_value})


class Company(Document):

    def __init__(self,
                 name: str = None,
                 employee_number: int = None,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.name = name
        self.employee_number = employee_number

    @classmethod
    def get_collection(cls) -> Collection:
        return col.COMPANY_COLLECTION

    @classmethod
    def match(cls, *matchers, **key_value_match):
        return DocumentQuery(
            aliases=[],
            attribute_return='',
            collection=cls.get_collection(),
            matchers=list(matchers),
            key_value_match=key_value_match
        )


col.COMPANY_COLLECTION.document_type = Company


class LocatedIn(Edge):

    def __init__(self,
                 since: datetime,
                 until: datetime,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.since = since
        self.until = until

    @classmethod
    def get_collection(cls) -> Collection:
        return col.LOCATED_AT


class Country(Document):

    def __init__(self,
                 name: str = None,
                 abbreviation: str = None,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.name = name
        self.abbreviation = abbreviation

    @classmethod
    def get_collection(cls) -> Collection:
        return col.COUNTRY_COLLECTION

    @classmethod
    def match(cls, *matchers: Filter, **key_value_match: Any):
        return DocumentQuery(
            aliases=[],
            attribute_return='',
            collection=cls.get_collection(),
            matchers=list(matchers),
            key_value_match=key_value_match
        )


col.LOCATED_AT.document_type = LocatedIn


def like(attribute: str, compare_value: Any) -> AttributeFilter:
    return AttributeFilter(attribute=attribute, operator='LIKE', compare_value=compare_value)


def eq(attribute: str, compare_value: Any) -> AttributeFilter:
    return AttributeFilter(attribute=attribute, operator='==', compare_value=compare_value)


def gt(attribute: str, compare_value: Any) -> AttributeFilter:
    return AttributeFilter(attribute=attribute, operator='>', compare_value=compare_value)


def out(*edge_collection_types: Type[Edge]) -> EdgeQuery:
    return EdgeQuery(
        edge_collections=[e.get_collection() for e in edge_collection_types],
        aliases=[],
        matchers=[],
        direction='outbound',
        previous_query=None
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
        return Stmt(f'''@{prefix}_0''', {f'{prefix}_0': self.expression}, result=alias_to_result[self.name])

    def _to_select_stmt(self, prefix: str, alias_to_result: Dict[str, Result], relative_to: str = '') -> Stmt:
        return Stmt(f'''@{prefix}_0''', {f'{prefix}_0': self.expression}, result=alias_to_result[self.name])


def var(expression: str):
    return Var(name=expression, attribute_return=expression)


def main():
    query = Company.match().as_var('a').select(var('a')) #.out(LocatedIn).as_var('b').select(a_object=var('a'), b_object=var('b'))  # .select(Document, countries_that_we_should_expand_to=Country.match(lead_industry=var('a').industry).name)

    client = ArangoClient()
    db = client.db('test', username='root', password='')

    query_stmt = query._to_stmt()
    query_str, bind_vars = query_stmt.expand()
    print(query_stmt.result)
    print(query_str)
    # result = db.aql.execute(query_str, bind_vars=bind_vars)
    # print(json.dumps(list(result), indent=4))


if __name__ == '__main__':
    main()
