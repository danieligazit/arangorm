from collections import defaultdict
from dataclasses import dataclass
from typing import List, TypeVar

from _direction import Direction
from _missing import MISSING
from _query import AttributeFilter
from _stmt import Stmt
from cursor._document_cursor import DocumentCursor
from new_query import HasEdge


class Document:
    INIT_PROPERTIES = ['_key', '_id', '_rev', '_db']

    def __init__(self, _key=None, _rev=None, _id=None, _db=None):
        self._key = _key
        self._rev = _rev
        self._id = _id
        self._db = _db

    @classmethod
    def _get_cursor(cls, db):
        return DocumentCursor(project=cls, collection=cls._get_collection(), db=db)

    @classmethod
    def _get_stmt(cls, prefix: str, max_recursion: defaultdict, relative_to: str = ''):
        if max_recursion.get(cls.__name__, max_recursion.default_factory()) == 0:
            return Stmt.expandable()

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
