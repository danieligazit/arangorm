from collections import defaultdict
from typing import List, Dict, Any, Tuple

from _missing import MISSING
from _stmt import Stmt
from cursor._edge_cursor import EdgeCursor
from cursor.filters._attribute_filter import AttributeFilter
from cursor.project._project import EdgeTarget


class EdgeEntity:
    INIT_PROPERTIES = ['_key', '_id', '_rev', '_from', '_to']
    IGNORED_PROPERTIES = ['_db']

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
    def _get_cursor(cls, db):
        return EdgeCursor(project=cls, collection=cls._get_collection(), db=db)

    @classmethod
    def _get_stmt(cls, prefix: str, max_recursion: defaultdict, relative_to: str = '', parent: 'Cursor' = None):
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

        return Stmt(f'''Merge({relative_to}, {{
                    {return_str}
                }})
            ''', bind_vars=bind_vars)

    @classmethod
    def _load(cls, result, db, id_to_doc: Dict[str, Any] = None):
        if not id_to_doc:
            id_to_doc = {}

        if result is None:
            return None

        target_schema = cls._get_target_schema()
        loaded = cls(**result, _db=db)
        id_to_doc[loaded._id] = loaded

        for key, target in target_schema.items():
            if key not in result:
                continue

            value = result[key]
            if value is False:
                setattr(loaded, key, MISSING)
                continue

            setattr(loaded, key, target._load(value, db, id_to_doc=id_to_doc))

        return loaded

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

    def _dump(self, covered_ids: List[str] = None) -> List[Tuple[str, Dict]]:
        if not covered_ids:
            covered_ids = []

        if self._id in covered_ids:
            return []

        result = vars(self)

        target_schema = self._get_target_schema()
        covered_ids.append(self._id)
        results = []

        for key, value in result.copy().items():
            if key in self.INIT_PROPERTIES and not value:
                result.pop(key)
                continue

            if key in self.IGNORED_PROPERTIES:
                result.pop(key)
                continue

            if key not in target_schema:
                continue

            if not value:
                continue

            results += value._dump(covered_ids=covered_ids)

            result.pop(key)

        return results + [(True, self._get_collection(), result)]
