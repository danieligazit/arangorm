from typing import List, Type, Dict

from _collection import Collection, EdgeCollection
from _document import Document, Edge
from new_query import HasEdge, EdgeEntity


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
