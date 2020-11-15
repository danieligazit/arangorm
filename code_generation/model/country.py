from dataclasses import dataclass
from document import Document
from aql_filter import Filter, EdgeFilter, AttributeFilter, attribute_filter, string_attribute_filter, EdgeFilterGenerator
from typing import List
import code_generation.model.collection_definition as col
from utils import classproperty
from collection import Collection


@dataclass
class CountryFilter:
    pass


class CountryEdgeFilter(EdgeFilter, CountryFilter):
    pass


class CountryAttributeFilter(AttributeFilter, CountryFilter):
    pass


@dataclass
class Country(Document):

    def __init__(self,
            name: str = None,
            abbreviation: str = None,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.name = name
        self.abbreviation = abbreviation

    @classmethod
    def get_collection(self) -> Collection:
        return col.COUNTRY_COLLECTION

    @classmethod
    @string_attribute_filter(CountryAttributeFilter)
    def by_name(
            cls,
            value: str = None,
            is_not: str = None,
            lt: str = None,
            lte: str = None,
            gt: str = None,
            gte: str = None,
            value_in: List[str] = None,
            not_in: List[str] = None,
            like: str = None,
            not_like: str = None,
            matches_regex: str = None,
            not_matches_regex: str = None
    ) -> Filter:
        pass

    @classmethod
    @string_attribute_filter(CountryAttributeFilter)
    def by_abbreviation(
            cls,
            value: str = None,
            is_not: str = None,
            lt: str = None,
            lte: str = None,
            gt: str = None,
            gte: str = None,
            value_in: List[str] = None,
            not_in: List[str] = None,
            like: str = None,
            not_like: str = None,
            matches_regex: str = None,
            not_matches_regex: str = None
    ) -> Filter:
        pass

