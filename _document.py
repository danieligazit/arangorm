from collections import defaultdict
from dataclasses import dataclass
from itertools import chain
from typing import List, TypeVar, Dict, Any, Tuple

from _direction import Direction
from _missing import MISSING
from _stmt import Stmt
from cursor._document_cursor import DocumentCursor
from cursor.project._project import HasEdge


class Document:
    INIT_PROPERTIES = ['_key', '_id', '_rev']
    IGNORED_PROPERTIES = ['_db']

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
            Merge({relative_to}, {{
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

        edge_value: HasEdge = edge_schema[item]

        if edge_value.direction == Direction.OUTBOUND:
            cursor = self._db.get(edge_value).match(_from=self._id)
        elif edge_value.direction == Direction.INBOUND:
            cursor = self._db.get(edge_value).match(_to=self._id)
        else:
            raise ValueError(f'Direction {edge_value.direction} is invalid')

        if edge_value.many:
            return cursor.all()

        return cursor.first()

    @classmethod
    def _load(cls, result, db, id_to_doc: Dict[str, Any] = None):

        if not id_to_doc:
            id_to_doc = {}

        if result is None:
            return None

        target_schema = cls._get_edge_schema()
        to_expand = []

        for key, target in target_schema.items():
            value = result.get(key)

            if key not in result or value is False:
                result[key] = MISSING
                continue

            to_expand.append((key, value, target))

        loaded = cls(**result, _db=db)
        id_to_doc[loaded._id] = loaded

        for key, value, target in to_expand:
            setattr(loaded, key, target._load(value, db, id_to_doc=id_to_doc))

        return loaded

    def _dump(self, covered_ids: List[str] = None) -> List[Tuple[bool, str, Dict]]:
        if not covered_ids:
            covered_ids = []

        if self._id in covered_ids:
            return []

        result = vars(self)
        covered_ids.append(result['_id'])
        edge_schema = self._get_edge_schema()
        results = []

        for key, value in list(result.items()):
            if key in self.INIT_PROPERTIES and not value:
                result.pop(key)
                continue

            if key in self.IGNORED_PROPERTIES:
                result.pop(key)
                continue

            if key not in edge_schema:
                continue

            if not value:
                continue

            if isinstance(value, list):
                results += chain.from_iterable([edge._dump(covered_ids=covered_ids) for edge in value])
            else:
                results += value._dump(covered_ids=covered_ids)

            result.pop(key)

        return results + [(False, self._get_collection(), result)]
