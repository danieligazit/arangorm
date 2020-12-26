from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Any

from _direction import Direction
from _missing import MISSING
from cursor._document_cursor import DocumentCursor
from cursor._edge_cursor import EdgeCursor
from cursor._str_to_type import STR_TO_TYPE
from _stmt import Stmt


class QueryStep(ABC):

    def _get_stmt(self, prefix: str, max_recursion: defaultdict, relative_to: str = '',
                  parent: 'Cursor' = None) -> Stmt:
        if max_recursion[self.target] == 0:
            return Stmt.expandable()

        max_recursion[self.target] = max_recursion[self.target] - 1

        return self._get_stmt_(prefix, max_recursion, relative_to, parent=parent)

    @abstractmethod
    def _get_stmt_(self, prefix: str, max_recursion: defaultdict, relative_to: str = '',
                   parent: 'Cursor' = None) -> Stmt:
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

    def _get_cursor(self, db):
        return DocumentCursor(project=self, collection=self._get_type()._get_collection(), db=db)

    def _get_stmt_(self, prefix: str, max_recursion: defaultdict, relative_to: str = '',
                   parent: 'Cursor' = None) -> Stmt:

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

        return Stmt(f'''MERGE({relative_to}, {{
                {return_str}
            }})
       ''', bind_vars=bind_vars)

    def _load(self, result, db, id_to_doc: Dict[str, Any] = None):
        if not id_to_doc:
            id_to_doc = {}

        previous_loaded = id_to_doc.get(result['_id'])
        if previous_loaded:
            return previous_loaded

        target_type = self._get_type()
        edge_schema = target_type._get_edge_schema()

        loaded = target_type(**result, _db=db)
        id_to_doc[loaded._id] = loaded

        for key, edge in edge_schema.items():
            if key not in result:
                continue

            value = result[key]
            if value is False:
                setattr(loaded, key, MISSING)
                continue

            setattr(loaded, key, edge._load(value, db, id_to_doc=id_to_doc))

        return loaded


@dataclass
class HasEdge(QueryStep):
    target: str
    direction: Direction
    many: bool = field(default=False)

    def _get_max_recursion(self):
        return STR_TO_TYPE[self.target]._get_max_recursion()

    def _get_type(self):
        return STR_TO_TYPE[self.target]

    def _get_stmt_(self, prefix: str, max_recursion: defaultdict, relative_to: str = '', parent: 'Cursor' = None):
        return_lines, bind_vars, index = [], {}, -1

        target_type = self._get_type()
        for attribute, annotation in target_type._get_target_schema().items():
            index += 1
            attribute_prefix = f'{prefix}_{index}'

            if isinstance(annotation, EdgeTarget):
                edge_target_stmt = annotation._get_stmt(attribute_prefix, max_recursion=max_recursion.copy(),
                                                        relative_to=f'v_{prefix}' if annotation._to else relative_to)
                edge_target_str, edge_target_vars = edge_target_stmt.expand()

                bind_vars.update(edge_target_vars)

                return_lines.append(f'@{attribute_prefix}: ({edge_target_str})')
                bind_vars[attribute_prefix] = attribute
                index += 1

        return_str = ',\n'.join(return_lines)

        traversal_id = relative_to

        if isinstance(parent, EdgeCursor):
            traversal_id += '._from' if self.direction is Direction.OUTBOUND else '._to'

        return Stmt(f'''(
                FOR v_{prefix}, e_{prefix} IN {self.direction} {traversal_id} {target_type._get_collection()}
                    RETURN MERGE(e_{prefix}, {{
                        {return_str}
                    }})

            ){'' if self.many else '[0]'}''', bind_vars=bind_vars)

    def _get_cursor(self, db):
        return EdgeCursor(project=self, collection=self._get_type()._get_collection(), db=db)

    def _load(self, result, db, id_to_doc: Dict[str, Any] = None):
        if not id_to_doc:
            id_to_doc = {}

        if not result:
            return

        if self.many:
            return [self._load_single(r, db, id_to_doc) for r in result]

        return self._load_single(result, db, id_to_doc)

    def _load_single(self, result, db, id_to_doc: Dict[str, Any]):
        previous_loaded = id_to_doc.get(result['_id'])
        if previous_loaded:
            return previous_loaded

        target_type = self._get_type()
        loaded = target_type(**result, _db=db)
        id_to_doc[loaded._id] = loaded

        for key, target in target_type._get_target_schema().items():
            if key not in result:
                continue

            value = result[key]

            if value is False:
                setattr(loaded, key, MISSING)
                continue

            setattr(loaded, key, target._load(value, db, id_to_doc))

        return loaded
