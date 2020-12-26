from typing import List, Type, Dict

from _collection import Collection, EdgeCollection
from _document import Document
from _edge_entity import EdgeEntity
from cursor._str_to_type import STR_TO_TYPE
from cursor.project._project import EdgeTarget, HasEdge


def edge(collection: str, from_collections: List[Collection] = None, to_collections: List[Collection] = None, target_schema: Dict[str, EdgeTarget] = None, max_recursion: Dict[str, int] = None):
    if not target_schema:
        target_schema = {}

    if not from_collections:
        from_collections = []

    if not to_collections:
        to_collections = []

    if not max_recursion:
        max_recursion = {}

    def class_creator(cls):
        class_dict = dict(cls.__dict__)

        def __init__(self, *args, **kwargs):
            this_kwargs, super_kwargs = {}, {}
            for key, value in kwargs.items():
                if key in EdgeEntity.INIT_PROPERTIES or key in EdgeEntity.IGNORED_PROPERTIES:
                    super_kwargs[key] = value
                else:
                    this_kwargs[key] = value

            EdgeEntity.__init__(self, **super_kwargs)
            cls.__init__(self, *args, **this_kwargs)

        class_dict['__init__'] = __init__

        @classmethod
        def _get_collection(_):
            return EdgeCollection(name=collection, from_collections=from_collections, to_collections=to_collections)

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
            content = []
            for key, value in vars(self).items():
                if key.startswith('_'):
                    continue

                class_name = value.__class__.__name__
                if class_name in STR_TO_TYPE:
                    content.append(f'{key}={value.__class__.__name__}(...)')
                    continue

                content.append(f'{key}={value}')
            return f'''{self.__class__.__qualname__}({', '.join(content)})'''

        class_dict['__repr__'] = __repr__

        del class_dict['__dict__']

        new_type = type(cls.__name__, (EdgeEntity,), class_dict)
        STR_TO_TYPE[cls.__name__] = new_type
        return new_type

    return class_creator


def document(collection: str, edge_schema: Dict[str, HasEdge] = None, max_recursion: Dict[str, int] = None):

    if not edge_schema:
        edge_schema = {}

    if not max_recursion:
        max_recursion = {}

    def class_creator(cls):
        class_dict = dict(cls.__dict__)

        def __init__(self, *args, **kwargs):
            this_kwargs, super_kwargs = {}, {}
            for key, value in kwargs.items():
                if key in Document.INIT_PROPERTIES or key in Document.IGNORED_PROPERTIES:
                    super_kwargs[key] = value
                else:
                    this_kwargs[key] = value

            Document.__init__(self, **super_kwargs)
            cls.__init__(self, *args, **this_kwargs)

        class_dict['__init__'] = __init__

        @classmethod
        def _get_collection(_):
            return Collection(name=collection)

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
            content = []
            for key, value in vars(self).items():
                if key.startswith('_'):
                    continue

                class_name = value.__class__.__name__
                if class_name in STR_TO_TYPE:
                    content.append(f'{key}={value.__class__.__name__}(...)')
                    continue

                content.append(f'{key}={value}')
            return f'''{self.__class__.__qualname__}({', '.join(content)})'''

        class_dict['__repr__'] = __repr__

        del class_dict['__dict__']
        new_type = type(cls.__name__, (Document,), class_dict)
        STR_TO_TYPE[cls.__name__] = new_type
        return new_type

    return class_creator
