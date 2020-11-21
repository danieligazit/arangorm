from dataclasses import dataclass
from datetime import datetime

from document import Document, Edge
from aql_filter import EdgeFilter, AttributeFilter
import model.collection_definition as col
from collection import Collection


@dataclass
class LocatedAtFilter:
    @classmethod
    def get_collection(cls) -> Collection:
        return col.COMPANY_COLLECTION


class LocatedAtAttributeFilter(AttributeFilter, LocatedAtFilter):
    pass


class LocatedAtEdge(Edge):
    @classmethod
    def get_collection(cls) -> Collection:
        return col.COMPANY_COLLECTION


@dataclass
class LocatedAt(LocatedAtEdge):

    def __init__(self,
            since: datetime,
            until: datetime,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.since = since
        self.until = until


col.LOCATED_AT.document_type = LocatedAt
