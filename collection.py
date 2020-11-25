from abc import ABC
from dataclasses import dataclass


@dataclass
class Collection(ABC):
    name: str
    document_type = None


@dataclass
class EdgeCollection(Collection):
    edge_filter_generator = None
    from_collection: Collection
    to_collection: Collection
