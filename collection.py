from abc import ABC
from dataclasses import dataclass


@dataclass
class Collection(ABC):
    name: str
    document_type = None


@dataclass
class EdgeCollection(Collection):
    from_collection: Collection
    to_collection: Collection
