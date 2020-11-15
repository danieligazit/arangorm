from dataclasses import dataclass
from document import Document
from aql_filter import Filter, EdgeFilter, AttributeFilter, attribute_filter, string_attribute_filter, EdgeFilterGenerator
from typing import List
import code_generation.model.collection_definition as col
from utils import classproperty
from collection import Collection


@dataclass
class CompanyFilter:
    pass


class CompanyEdgeFilter(EdgeFilter, CompanyFilter):
    pass


class CompanyAttributeFilter(AttributeFilter, CompanyFilter):
    pass


@dataclass
class Company(Document):

    def __init__(self,
            name: str = None,
            employee_number: int = None,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.name = name
        self.employee_number = employee_number

    @classmethod
    def get_collection(self) -> Collection:
        return col.COMPANY_COLLECTION

    @classmethod
    @string_attribute_filter(CompanyAttributeFilter)
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
    @attribute_filter(CompanyAttributeFilter)
    def by_employee_number(
            cls,
            value: int = None,
            is_not: int = None,
            lt: int = None,
            lte: int = None,
            gt: int = None,
            gte: int = None,
            value_in: List[int] = None,
            not_in: List[int] = None    ) -> Filter:
        pass

    @classproperty
    def located_at(cls):
        return EdgeFilterGenerator(col.LOCATED_AT, None, CompanyEdgeFilter)
