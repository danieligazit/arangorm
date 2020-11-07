from dataclasses import dataclass
from document import Collection


@dataclass
class Relation:
    collection_from: Collection
    collection_name: str
    collection_to: Collection
