import datetime
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
class MatchObject:
    attribute: str
    operator: str
    compare_value: Any


class CompanyDocument(Document):
    @classmethod
    def get_collection(cls) -> Collection:
        return col.COMPANY_COLLECTION

    def match(self, **kwargs):
        return
    def filter_by(self, prefix: str = 'p', depth: int = 0) -> Tuple[str, Dict[str, Any]]:
        return AttributeFilter(self.get_collection(), None, '_key', '==', self._key).filter_by(prefix, depth)


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

