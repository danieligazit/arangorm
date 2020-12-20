from dataclasses import dataclass
from typing import List, Type


@dataclass
class Collection:
    name: str
    document_type: Type['Document']


@dataclass
class EdgeCollection(Collection):
    edge_filter_generator = None
    from_collections: List[Collection]
    to_collections: List[Collection]
