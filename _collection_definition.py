from typing import List, Type

from _collection import Collection, EdgeCollection
from _document import Document, Edge


def collection(collection_name: str):
    def create_class(_cls):

        class_dict = dict(_cls.__dict__)

        def __init__(self, *args, **kwargs):
            this_kwargs, super_kwargs = {}, {}
            for key, value in kwargs.items():
                if key in Document.INIT_PROPERTIES:
                    super_kwargs[key] = value
                else:
                    this_kwargs[key] = value

            Document.__init__(self, **super_kwargs)
            _cls.__init__(self, *args, **this_kwargs)

        class_dict['__init__'] = __init__

        @classmethod
        def _get_collection(cls):
            return Collection(name=collection_name, document_type=cls)

        class_dict['_get_collection'] = _get_collection
        del class_dict['__dict__']
        NewClass = type(_cls.__name__, (Document,), class_dict)

        return NewClass

    return create_class


def edge_collection(collection_name: str, from_collections: List[Type], to_collections: List[Type]):
    def create_class(_cls):

        class_dict = dict(_cls.__dict__)

        def __init__(self, *args, **kwargs):
            this_kwargs, super_kwargs = {}, {}
            for key, value in kwargs.items():
                if key in Document.INIT_PROPERTIES:
                    super_kwargs[key] = value
                else:
                    this_kwargs[key] = value

            Edge.__init__(self, **super_kwargs)
            _cls.__init__(self, *args, **this_kwargs)

        class_dict['__init__'] = __init__

        @classmethod
        def _get_collection(cls):
            return EdgeCollection(name=collection_name, document_type=cls,
                                  from_collections=[c._get_collection() for c in from_collections],
                                  to_collections=[c._get_collection() for c in to_collections])

        class_dict['_get_collection'] = _get_collection
        del class_dict['__dict__']
        NewClass = type(_cls.__name__, (Edge,), class_dict)

        return NewClass

    return create_class