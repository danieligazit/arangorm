from abc import abstractmethod, ABC
from typing import Type


class Document(ABC):
    def __init__(self, _key=None, _rev=None, _id=None):
        self._key = _key
        self._rev = _rev
        self._id = _id

    @classmethod
    @abstractmethod
    def get_collection_name(self):
        pass

    @abstractmethod
    def get_collection(self):
        pass

    @classmethod
    def _load(cls, d: dict):
        return cls(**d)

    def _dump(self):
        result = vars(self)

        for attribute in ['_key', '_rev', '_id']:
            if getattr(self, attribute) is None:
                result.pop(attribute)

        return result

    def __repr__(self):
        return f'{type(self)}{vars(self)}'


Collection = Type[Document]
