from abc import abstractmethod, ABC
from collection import Collection


class Document(ABC):
    def __init__(self, _key=None, _rev=None, _id=None):
        self._key = _key
        self._rev = _rev
        self._id = _id

    @abstractmethod
    def get_collection(self) -> Collection:
        pass

    @classmethod
    def _load(cls, d: dict) -> 'Document':
        return cls(**d)

    def _dump(self) -> dict:
        result = vars(self)

        for attribute in ['_key', '_rev', '_id']:
            if getattr(self, attribute) is None:
                result.pop(attribute)

        return result

    def __repr__(self) -> str:
        return f'{type(self)}{vars(self)}'
