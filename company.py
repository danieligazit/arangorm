from dataclasses import dataclass
from document import Document
from aql_filter import Filter, EdgeFilter, AttributeFilter
from collection_definitions import COMPANY_COLLECTION


@dataclass
class CompanyFilter(Filter):
    def by_name(self, name):
        return CompanyAttributeFilter(COMPANY_COLLECTION, self, attribute='name', operator='==', compare_value=name)


class CompanyEdgeFilter(EdgeFilter, CompanyFilter):
    pass


class CompanyAttributeFilter(AttributeFilter, CompanyFilter):
    pass


class Company(Document):
    def __init__(self, name: str, **kwargs):
        super().__init__(**kwargs)
        self.name = name

    def get_collection(self):
        return COMPANY_COLLECTION

    @classmethod
    def by_name(cls, name: str):
        return CompanyAttributeFilter(COMPANY_COLLECTION, None, attribute='name', operator='==', compare_value=name)


COMPANY_COLLECTION.document_type = Company
