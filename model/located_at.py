from dataclasses import dataclass
from datetime import datetime
from typing import List

from document import Document, Edge
from aql_filter import EdgeFilter, AttributeFilter, EdgeFilterGenerator, string_attribute_filter
import model.collection_definition as col
from collection import Collection


@dataclass
class LocatedAtFilter:
    @classmethod
    def get_collection(cls) -> Collection:
        return col.LOCATED_AT

    @string_attribute_filter(attribute='since')
    def by_since(self,
            value: str = None,
            value_not: str = None,
            value_in: List[str] = None,
            not_in: List[str] = None,
            like: str = None,
            not_like: str = None,
            matches_regex: str = None,
            not_matches_regex: str = None
    ):
        return LocatedAtAttributeFilter


class LocatedAtAttributeFilter(AttributeFilter, LocatedAtFilter):
    pass


class LocatedAtEdgeFilterGenerator(EdgeFilterGenerator, LocatedAtFilter):
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
col.LOCATED_AT.edge_filter_generator = LocatedAtEdgeFilterGenerator
