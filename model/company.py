from dataclasses import dataclass
from document import Document
from aql_filter import Filter, EdgeFilter, AttributeFilter, EdgeFilterGenerator, string_attribute_filter, \
    comparable_attribute_filter, default_attribute_filter
from typing import List, Dict, Tuple
from typing import Any
import model.collection_definition as col
from utils import classproperty
from collection import Collection


@dataclass
class CompanyFilter:
    @classmethod
    def get_collection(cls) -> Collection:
        return col.COMPANY_COLLECTION

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
    ) -> 'CompanyAttributeFilter':
        return CompanyAttributeFilter

    @comparable_attribute_filter(attribute='employee_number')
    def by_employee_number(
            self,
            value: int = None,
            is_not: int = None,
            lt: int = None,
            lte: int = None,
            gt: int = None,
            gte: int = None,
            value_in: List[int] = None,
            not_in: List[int] = None
    ) -> 'CompanyAttributeFilter':
        return CompanyAttributeFilter

    @property
    def located_at(self):
        return EdgeFilterGenerator(col.LOCATED_AT, self, CompanyEdgeFilter)


class CompanyEdgeFilter(EdgeFilter, CompanyFilter):
    pass


class CompanyAttributeFilter(AttributeFilter, CompanyFilter):
    pass


class CompanyDocument(Document):
    @classmethod
    def get_collection(cls) -> Collection:
        return col.COMPANY_COLLECTION

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
    ) -> CompanyAttributeFilter:
        return CompanyAttributeFilter

    @classmethod
    @comparable_attribute_filter(attribute='employee_number')
    def by_employee_number(
            cls,
            value: int = None,
            is_not: int = None,
            lt: int = None,
            lte: int = None,
            gt: int = None,
            gte: int = None,
            value_in: List[int] = None,
            not_in: List[int] = None
    ) -> CompanyAttributeFilter:
        return CompanyAttributeFilter

    @classproperty
    def located_at(cls):
        return EdgeFilterGenerator(col.LOCATED_AT, cls, CompanyEdgeFilter)

    def filter_by(self, prefix: str = 'p', depth: int = 0) -> Tuple[str, Dict[str, Any]]:
        return CompanyAttributeFilter(self.get_collection(), None, '_key', '==', self._key).filter_by(prefix, depth)

@dataclass
class Company(CompanyDocument):

    def __init__(self,
                 name: str = None,
                 employee_number: int = None,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.name = name
        self.employee_number = employee_number


col.COMPANY_COLLECTION.document_type = Company

