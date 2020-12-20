from _query import AttributeFilter
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List

from _stmt import Stmt
from new_db import DB


class _MISSING_TYPE:
    def __repr__(self):
        return '<Expandable>'


MISSING = _MISSING_TYPE()


class Direction:
    INBOUND = 'INBOUND'
    OUTBOUND = 'OUTBOUND'


class QueryStep:

    def _get_stmt(self, prefix: str, max_recursion: defaultdict, relative_to: str = '') -> Stmt:
        if max_recursion[self.target] == 0:
            return Stmt.expandable()

        max_recursion[self.target] = max_recursion[self.target] - 1

        return self._get_stmt_(prefix, max_recursion, relative_to)

    def _get_stmt_(self, prefix: str, max_recursion: defaultdict, relative_to: str = '') -> Stmt:
        pass


@dataclass
class EdgeTarget(QueryStep):
    target: str
    max_recursion: Dict[str, int] = field(default_factory=dict)
    _to: bool = field(default=True)

    def _get_max_recursion(self):
        return self.max_recursion

    def _get_type(self):
        return STR_TO_TYPE[self.target]

    def _to_stmt(self, prefix: str, matchers: List[AttributeFilter] = None):

        if not matchers:
            matchers = []

        matcher_lines, bind_vars, index = [], {}, 0

        returns = f'o_{prefix}'

        for matcher in matchers:
            query_stmt, matcher_vars = matcher._to_filter_stmt(prefix=f'{prefix}_{index}',
                                                               relative_to=returns).expand_without_return()
            matcher_lines.append(query_stmt)
            bind_vars.update(matcher_vars)
            index += len(matcher_vars)

        matchers_str = ',\n'.join(matcher_lines)
        return_stmt = self._get_stmt(prefix=f'{prefix}_re',
                                     max_recursion=defaultdict(lambda: 1, self._get_max_recursion()),
                                     relative_to=returns)
        return_str, return_vars = return_stmt.expand()
        bind_vars.update(return_vars)

        return Stmt(f'''
            FOR {returns} IN {self._get_type()._get_collection()}
                {matchers_str}
                RETURN {return_str}
            ''', bind_vars=bind_vars)

    def _get_stmt_(self, prefix: str, max_recursion: defaultdict, relative_to: str = '') -> Stmt:

        return_lines = []
        bind_vars = {}
        index = 0

        for attribute, annotation in self._get_type()._get_edge_schema().items():
            if not isinstance(annotation, HasEdge):
                continue

            attribute_prefix = f'{prefix}_{index}'
            attribute_stmt = annotation._get_stmt(attribute_prefix, max_recursion=max_recursion.copy(),
                                                  relative_to=relative_to)
            attribute_str, attribute_bind_vars = attribute_stmt.expand()
            bind_vars.update(attribute_bind_vars)

            return_lines.append(f'@{attribute_prefix}: ({attribute_str})')
            bind_vars[attribute_prefix] = attribute
            index += 1

        if not return_lines:
            return Stmt(relative_to)

        return_str = ',\n'.join(return_lines)

        return Stmt(f'''
            Merge({relative_to}, {{
                {return_str}
            }})
       ''', bind_vars=bind_vars)

    def _load(self, result, db):
        target_type = self._get_type()
        edge_schema = target_type._get_edge_schema()

        for key, edge in edge_schema.items():
            if key not in result:
                continue

            value = result[key]
            if value is False:
                result[key] = MISSING
                continue

            result[key] = edge._load(value, db)

        return target_type(**result, _db=db)


@dataclass
class HasEdge(QueryStep):
    target: str
    direction: str
    many: bool = field(default=False)

    def _get_type(self):
        return STR_TO_TYPE[self.target]

    def _get_stmt_(self, prefix: str, max_recursion: defaultdict, relative_to: str = ''):
        return_lines, bind_vars, index = [], {}, -1

        target_type = self._get_type()
        for attribute, annotation in target_type._get_target_schema().items():
            index += 1
            attribute_prefix = f'{prefix}_{index}'

            if isinstance(annotation, EdgeTarget):
                edge_target_stmt = annotation._get_stmt(attribute_prefix, max_recursion=max_recursion.copy(),
                                                        relative_to=f'v_{prefix}')
                edge_target_str, edge_target_vars = edge_target_stmt.expand()

                bind_vars.update(edge_target_vars)

                return_lines.append(f'@{attribute_prefix}: ({edge_target_str})')
                bind_vars[attribute_prefix] = attribute
                index += 1

        return_str = ',\n'.join(return_lines)

        return Stmt(f'''(
                FOR v_{prefix}, e_{prefix} IN {self.direction} {relative_to}._id {target_type._get_collection()}
                    RETURN MERGE(e_{prefix}, {{
                        {return_str}
                    }})

            ){'' if self.many else '[0]'}''', bind_vars=bind_vars)

    def _load(self, result, db):
        if not result:
            return

        if self.many:
            return [self._load_single(r, db) for r in result]

        return self._load_single(result, db)

    def _load_single(self, result, db):
        target_type = self._get_type()

        for key, target in target_type._get_target_schema().items():
            if key not in result:
                continue

            value = result[key]

            if value is False:
                result[key] = MISSING
                continue

            result[key] = target._load(value, db)

        return target_type(**result, _db=db)


class Document:
    INIT_PROPERTIES = ['_key', '_id', '_rev', '_db']

    def __init__(self, _key=None, _rev=None, _id=None, _db=None):
        self._key = _key
        self._rev = _rev
        self._id = _id
        self._db = _db

    @classmethod
    def _to_stmt(cls, prefix: str, matchers: List[AttributeFilter] = None):
        if not matchers:
            matchers = []

        matcher_lines, bind_vars, index = [], {}, 0

        returns = f'o_{prefix}'

        for matcher in matchers:
            query_stmt, matcher_vars = matcher._to_filter_stmt(prefix=f'{prefix}_{index}',
                                                               relative_to=returns).expand_without_return()
            matcher_lines.append(query_stmt)
            bind_vars.update(matcher_vars)
            index += len(matcher_vars)

        matchers_str = ',\n'.join(matcher_lines)
        return_stmt = cls._get_stmt(prefix=f'{prefix}_re',
                                    max_recursion=defaultdict(lambda: 1, cls._get_max_recursion()),
                                    relative_to=returns)
        return_str, return_vars = return_stmt.expand()
        bind_vars.update(return_vars)

        return Stmt(f'''
            FOR {returns} IN {cls._get_collection()}
                {matchers_str}
                {return_str}
            ''', bind_vars=bind_vars)

    @classmethod
    def _get_stmt(cls, prefix: str, max_recursion: defaultdict, relative_to: str = ''):
        if max_recursion.get(cls.__name__, max_recursion.default_factory()) == 0:
            return Stmt('False')

        max_recursion[cls.__name__] = max_recursion[cls.__name__] - 1

        return_lines, bind_vars, index = [], {}, 0

        for attribute, annotation in cls._get_edge_schema().items():
            if not isinstance(annotation, HasEdge):
                continue

            attribute_prefix = f'{prefix}_{index}'

            attribute_stmt = annotation._get_stmt(attribute_prefix, max_recursion=max_recursion.copy(),
                                                  relative_to=relative_to)
            attribute_str, attribute_bind_vars = attribute_stmt.expand()
            bind_vars.update(attribute_bind_vars)

            return_lines.append(f'@{attribute_prefix}: ({attribute_str})')
            bind_vars[attribute_prefix] = attribute
            index += 1

        return_str = ',\n'.join(return_lines)

        return Stmt(f'''
            RETURN Merge({relative_to}, {{
                {return_str}
            }})
        ''', bind_vars=bind_vars)

    def __getattribute__(self, item):
        edge_schema = object.__getattribute__(self, '_get_edge_schema')()

        value = object.__getattribute__(self, item)

        if item not in edge_schema:
            return value

        if value is not MISSING:
            return value

        edge_value: HasEdge = edge_schema[value]

        if edge_value.direction == Direction.OUTBOUND:
            cursor = self._db.get(edge_value).match(_from=self._id)
        elif edge_value.direction == Direction.INBOUND:
            cursor = self._db.get(edge_value).match(_to=self._id)
        else:
            raise ValueError(f'Direction {edge_value.direction} is invalid')

        if edge_value.many:
            return cursor.all()
        return next(cursor, None)

    @classmethod
    def _load(cls, result, db):
        edge_schema = cls._get_edge_schema()

        for key, edge in edge_schema.items():
            if key not in result:
                continue

            value = result[key]
            if value is False:
                result[key] = MISSING
                continue

            result[key] = edge._load(value, db)

        return cls(**result, _db=db)


class EdgeEntity:
    INIT_PROPERTIES = ['_key', '_id', '_rev', '_from', '_to', '_db']

    def __init__(self, _key=None, _rev=None, _id=None, _from=None, _to=None, _db=None):
        self._key = _key
        self._rev = _rev
        self._id = _id
        self._from = _from
        self._to = _to
        self._db = _db

    @classmethod
    def _to_stmt(cls, prefix: str, matchers: List[AttributeFilter] = None):
        if not matchers:
            matchers = []

        matcher_lines, bind_vars, index = [], {}, 0

        returns = f'o_{prefix}'

        for matcher in matchers:
            query_stmt, matcher_vars = matcher._to_filter_stmt(prefix=f'{prefix}_{index}',
                                                               relative_to=returns).expand_without_return()
            matcher_lines.append(query_stmt)
            bind_vars.update(matcher_vars)
            index += len(matcher_vars)

        matchers_str = ',\n'.join(matcher_lines)
        return_stmt = cls._get_stmt(prefix=f'{prefix}_re',
                                    max_recursion=defaultdict(lambda: 1, cls._get_max_recursion()),
                                    relative_to=returns)
        return_str, return_vars = return_stmt.expand()
        bind_vars.update(return_vars)

        return Stmt(f'''
                FOR {returns} IN {cls._get_collection()}
                    {matchers_str}
                    {return_str}
                ''', bind_vars=bind_vars)

    @classmethod
    def _get_stmt(cls, prefix: str, max_recursion: defaultdict, relative_to: str = ''):
        if max_recursion.get(cls.__name__, max_recursion.default_factory()) == 0:
            return Stmt('False')

        max_recursion[cls.__name__] = max_recursion[cls.__name__] - 1
        return_lines, bind_vars, index = [], {}, 0

        for attribute, annotation in cls._get_target_schema().items():
            if not isinstance(annotation, EdgeTarget):
                continue

            annotation: EdgeTarget

            attribute_prefix = f'{prefix}_{index}'
            attribute_stmt = annotation._get_stmt(attribute_prefix, max_recursion=max_recursion.copy(),
                                                  relative_to=f'{attribute_prefix}_o')
            bind_vars[attribute_prefix] = attribute
            index += 1

            if attribute_stmt.is_expandable:
                return_lines.append(f'''@{attribute_prefix}: False''')
                continue

            attribute_str, attribute_bind_vars = attribute_stmt.expand()
            bind_vars.update(attribute_bind_vars)

            return_lines.append(f'''@{attribute_prefix}: (
                FOR {attribute_prefix}_o IN {annotation._get_type()._get_collection()}
                    FILTER {attribute_prefix}_o._id == {relative_to}.{'_to' if annotation._to else '_from'}
                    RETURN {attribute_str}
            )[0]''')

        return_str = ',\n'.join(return_lines)

        return Stmt(f'''
                RETURN Merge({relative_to}, {{
                    {return_str}
                }})
            ''', bind_vars=bind_vars)

    @classmethod
    def _load(cls, result, db):
        target_schema = cls._get_target_schema()

        for key, target in target_schema.items():
            if key not in result:
                continue

            value = result[key]
            if value is False:
                result[key] = MISSING
                continue

            result[key] = target._load(value, db)

        return cls(**result, _db=db)

    def __getattribute__(self, item):
        target_schema = object.__getattribute__(self, '_get_target_schema')()
        value = object.__getattribute__(self, item)

        if item not in target_schema:
            return value

        if value is not MISSING:
            return value

        edge_value: EdgeTarget = target_schema[item]

        cursor = self._db.get(edge_value).match(_id=self._to if edge_value._to else self._from)

        return cursor.first()


def edge(collection: str, target_schema: Dict[str, HasEdge], max_recursion: Dict[str, int] = None):
    def class_creator(cls):
        class_dict = dict(cls.__dict__)

        def __init__(self, *args, **kwargs):
            this_kwargs, super_kwargs = {}, {}
            for key, value in kwargs.items():
                if key in EdgeEntity.INIT_PROPERTIES:
                    super_kwargs[key] = value
                else:
                    this_kwargs[key] = value

            EdgeEntity.__init__(self, **super_kwargs)
            cls.__init__(self, *args, **this_kwargs)

        class_dict['__init__'] = __init__

        @classmethod
        def _get_collection(_):
            return collection

        class_dict['_get_collection'] = _get_collection

        @classmethod
        def _get_max_recursion(_):
            return max_recursion

        class_dict['_get_max_recursion'] = _get_max_recursion

        @classmethod
        def _get_target_schema(_):
            return target_schema

        class_dict['_get_target_schema'] = _get_target_schema

        def __repr__(self):
            content = [f'{key}={value}' for key, value in vars(self).items() if not key.startswith('_')]
            return f'''{self.__class__.__qualname__}({', '.join(content)})'''

        class_dict['__repr__'] = __repr__

        del class_dict['__dict__']
        return type(cls.__name__, (EdgeEntity,), class_dict)

    return class_creator


def document(collection: str, edge_schema: Dict[str, HasEdge], max_recursion: Dict[str, int] = None):
    if not max_recursion:
        max_recursion = {}

    def class_creator(cls):
        class_dict = dict(cls.__dict__)

        def __init__(self, *args, **kwargs):
            this_kwargs, super_kwargs = {}, {}
            for key, value in kwargs.items():
                if key in Document.INIT_PROPERTIES:
                    super_kwargs[key] = value
                else:
                    this_kwargs[key] = value

            Document.__init__(self, **super_kwargs)
            cls.__init__(self, *args, **this_kwargs)

        class_dict['__init__'] = __init__

        @classmethod
        def _get_collection(_):
            return collection

        class_dict['_get_collection'] = _get_collection

        @classmethod
        def _get_max_recursion(_):
            return max_recursion

        class_dict['_get_max_recursion'] = _get_max_recursion

        @classmethod
        def _get_edge_schema(_):
            return edge_schema

        class_dict['_get_edge_schema'] = _get_edge_schema

        def __repr__(self):
            content = [f'{key}={value}' for key, value in vars(self).items() if not key.startswith('_')]
            return f'''{self.__class__.__qualname__}({', '.join(content)})'''

        class_dict['__repr__'] = __repr__

        del class_dict['__dict__']
        return type(cls.__name__, (Document,), class_dict.copy())

    return class_creator


@document(
    collection='company',
    edge_schema={
        'subsidiary_of': HasEdge('Subsidiary', direction='outbound', many=False),
        'has_subsidiaries': HasEdge('Subsidiary', direction='inbound', many=True)

    },
    max_recursion={
        'Company': 0
    }
)
@dataclass
class Company:
    name: str
    subsidiary_of: 'Subsidiary'
    has_subsidiaries: List['Subsidiary']
    employee_number: int
    industry: str


@edge(
    collection='subsidiary_of',
    target_schema={
        'parent': EdgeTarget('Company', _to=True),
        'daughter': EdgeTarget('Company', _to=False)
    },
    max_recursion={
        'Company': 2, 'Subsidiary': 5
    }
)
@dataclass
class Subsidiary:
    since: str
    until: str
    parent: Company
    daughter: Company


STR_TO_TYPE = {
    'Company': Company,
    'Subsidiary': Subsidiary,
}

if __name__ == '__main__':
    new_db = DB(username='root', password='', db_name='test')

    result = new_db.get(Subsidiary)
    print(result.first())
