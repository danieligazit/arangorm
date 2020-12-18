from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from sys import getsizeof
from typing import Dict

from _stmt import Stmt
from new_db import DB


class _MISSING_TYPE:
    pass


MISSING = _MISSING_TYPE()


class Direction:
    INBOUND = 'INBOUND'
    OUTBOUND = 'OUTBOUND'


class QueryStep:

    def _get_stmt(self, prefix: str, max_recursion: defaultdict, relative_to: str = '') -> Stmt:
        if max_recursion[self.target] == 0:
            return Stmt('null')

        max_recursion[self.target] = max_recursion[self.target] - 1

        return self._get_stmt_(prefix, max_recursion, relative_to)

    def _get_stmt_(self, prefix: str, max_recursion: defaultdict, relative_to: str = '') -> Stmt:
        pass


@dataclass
class EdgeTarget(QueryStep):
    target: str

    def _get_type(self):
        return STR_TO_TYPE[self.target]

    def _get_stmt_(self, prefix: str, max_recursion: defaultdict, relative_to: str = '') -> Stmt:

        return_lines = []
        bind_vars = {}
        index = 0

        for attribute, annotation in self._get_type()._get_edge_schema().items():
            if not isinstance(annotation, Edge):
                continue

            attribute_prefix = f'{prefix}_{index}'
            attribute_stmt = annotation._get_stmt(attribute_prefix, max_recursion=max_recursion.copy(),
                                                  relative_to=relative_to)
            attribute_str, attribute_bind_vars = attribute_stmt.expand()
            bind_vars.update(attribute_bind_vars)

            return_lines.append(f'{attribute_prefix}: ({attribute_str})')
            bind_vars[attribute_prefix] = attribute
            index += 1

        if not return_lines:
            return Stmt(relative_to)

        return_str = ',\n'.join(return_lines)

        return Stmt(f'''
            RETURN Merge({relative_to}, {{
                {return_str}
            }}
        ''', bind_vars=bind_vars)

    def _load(self, result):
        return None


@dataclass
class Edge:
    target: str
    direction: str
    many: bool = field(default=False)

    def _get_type(self):
        return STR_TO_TYPE[self.target]

    def _get_stmt(self, prefix: str, max_recursion: defaultdict, relative_to: str = ''):
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

    def _load(self, result):
        if not result:
            return

        target_type = self._get_type()

        for key, target in target_type._get_target_schema().items():
            if key not in result:
                continue

            result[key] = target._load(result)

        return target_type(**result)


class DocumentEntity:
    INIT_PROPERTIES = ['_key', '_id', '_rev', '_db']

    def __init__(self, _key=None, _rev=None, _id=None, _db=None):
        self._key = _key
        self._rev = _rev
        self._id = _id
        self._db = _db

    @classmethod
    def _get_stmt(cls, prefix: str, max_recursion: defaultdict, relative_to: str = ''):

        if max_recursion[cls.__name__] == 0:
            return Stmt('null')

        max_recursion[cls.__name__] = max_recursion[cls.__name__] - 1

        return_lines = []
        bind_vars = {}
        index = 0
        returns = f'o_{prefix}'

        for attribute, annotation in cls._get_edge_schema().items():
            if not isinstance(annotation, Edge):
                continue

            attribute_prefix = f'{prefix}_{index}'

            attribute_stmt = annotation._get_stmt(attribute_prefix, max_recursion=max_recursion.copy(),
                                                  relative_to=returns)
            attribute_str, attribute_bind_vars = attribute_stmt.expand()
            bind_vars.update(attribute_bind_vars)

            return_lines.append(f'@{attribute_prefix}: ({attribute_str})')
            bind_vars[attribute_prefix] = attribute
            index += 1

        return_str = ',\n'.join(return_lines)

        return Stmt(f'''
            FOR {returns} IN {cls._get_collection()}
                RETURN Merge({returns}, {{
                    {return_str}
                }})
        ''', bind_vars=bind_vars)

    def __getattribute__(self, item):
        edge_schema = self._get_edge_schema()
        value = object.__getattribute__(self, item)

        if item not in edge_schema:
            return value

        if value is not MISSING:
            return value

        edge_value: Edge = edge_schema[value]

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

            result[key] = edge._load(result[key])

        result['_key'] = db
        return cls(**result)


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
    def _load(cls, result):
        edge_schema = cls._get_edge_schema()

        for key, edge in edge_schema.items():
            if key not in result:
                continue

            result[key] = edge._load(result)

        return cls(**result)

    def __getattribute__(self, item):
        target_schema = self._get_target_schema()
        value = object.__getattribute__(self, item)

        if item not in target_schema:
            return value

        if value is not MISSING:
            return value

        edge_value: Edge = target_schema[value]

        cursor = self._db.get(edge_value).match(_id=self._to)

        if edge_value.many:
            return cursor.all()

        return next(cursor, None)


def edge(collection: str, target_schema: Dict[str, Edge]):
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
        def _get_target_schema(_):
            return target_schema

        class_dict['_get_target_schema'] = _get_target_schema

        return type(cls.__name__, (EdgeEntity,), class_dict)

    return class_creator


def document(collection: str, edge_schema: Dict[str, Edge], max_recursive: Dict[str, int]):
    def class_creator(cls):
        class_dict = dict(cls.__dict__)

        def __init__(self, *args, **kwargs):
            this_kwargs, super_kwargs = {}, {}
            for key, value in kwargs.items():
                if key in DocumentEntity.INIT_PROPERTIES:
                    super_kwargs[key] = value
                else:
                    this_kwargs[key] = value

            DocumentEntity.__init__(self, **super_kwargs)
            cls.__init__(self, *args, **this_kwargs)

        class_dict['__init__'] = __init__

        @classmethod
        def _get_collection(_):
            return collection

        class_dict['_get_collection'] = _get_collection

        @classmethod
        def _get_max_recursive(_):
            return max_recursive

        class_dict['_get_max_recursive'] = _get_max_recursive

        @classmethod
        def _get_edge_schema(_):
            return edge_schema

        class_dict['_get_edge_schema'] = _get_edge_schema

        return type(cls.__name__, (DocumentEntity,), class_dict)

    return class_creator


@document(
    collection='company',
    edge_schema={
        'subsidiary': Edge('Subsidiary', direction='outbound', many=False)
    },
    max_recursive={
        'Company': 1
    },
    # force_schema=False
)
@dataclass
class Company:
    name: str
    subsidiary: 'Subsidiary'
    employee_number: int
    industry: str


@edge(
    collection='subsidiary_of',
    target_schema={
        'of': EdgeTarget('Company')
    }
)
@dataclass
class Subsidiary:
    since: str
    until: str
    of: Company


STR_TO_TYPE = {
    'Company': Company,
    'Subsidiary': Subsidiary,
}

if __name__ == '__main__':
    print(Company._get_max_recursive())

    q_str, bind_vars = Company._get_stmt(prefix='p',
                                         max_recursion=defaultdict(lambda: 1, Company._get_max_recursive())).expand()

    new_db = DB(username='root', password='', db_name='test')
    print(q_str)
    print(bind_vars)

    result = new_db.get(Company)
    print(getsizeof(result))
