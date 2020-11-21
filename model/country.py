from dataclasses import dataclass
from document import Document
from aql_filter import Filter, EdgeFilter, AttributeFilter, EdgeFilterGenerator, string_attribute_filter, \
    comparable_attribute_filter, default_attribute_filter
from typing import List
from typing import Any
import model.collection_definition as col
from utils import classproperty
from collection import Collection


@dataclass
class CountryFilter:
    @classmethod
    def get_collection(cls) -> Collection:
        return col.COUNTRY_COLLECTION

    @string_attribute_filter(attribute='name')
    def by_name(
            self,
            value: str = None,
            is_not: str = None,
            value_in: List[str] = None,
            not_in: List[str] = None,
            like: str = None,
            not_like: str = None,
            matches_regex: str = None,
            not_matches_regex: str = None
    ) -> 'CountryAttributeFilter':
        return CountryAttributeFilter

    @string_attribute_filter(attribute='abbreviation')
    def by_abbreviation(
            self,
            value: str = None,
            is_not: str = None,
            value_in: List[str] = None,
            not_in: List[str] = None,
            like: str = None,
            not_like: str = None,
            matches_regex: str = None,
            not_matches_regex: str = None
    ) -> 'CountryAttributeFilter':
        return CountryAttributeFilter


class CountryEdgeFilter(EdgeFilter, CountryFilter):
    pass


class CountryAttributeFilter(AttributeFilter, CountryFilter):
    pass


class CountryDocument(Document):
    @classmethod
    def get_collection(cls) -> Collection:
        return col.COUNTRY_COLLECTION

    @classmethod
    @string_attribute_filter(attribute='name')
    def by_name(
            cls,
            value: str = None,
            is_not: str = None,
            value_in: List[str] = None,
            not_in: List[str] = None,
            like: str = None,
            not_like: str = None,
            matches_regex: str = None,
            not_matches_regex: str = None
    ) -> CountryAttributeFilter:
        return CountryAttributeFilter

    @classmethod
    @string_attribute_filter(attribute='abbreviation')
    def by_abbreviation(
            cls,
            value: str = None,
            is_not: str = None,
            value_in: List[str] = None,
            not_in: List[str] = None,
            like: str = None,
            not_like: str = None,
            matches_regex: str = None,
            not_matches_regex: str = None
    ) -> CountryAttributeFilter:
        return CountryAttributeFilter


@dataclass
class Country(CountryDocument):

    def __init__(self,
                 name: str = None,
                 abbreviation: str = None,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.name = name
        self.abbreviation = abbreviation


col.COUNTRY_COLLECTION.document_type = Country
