from datetime import datetime

from collection import EdgeCollection, Collection
from document import Document, Edge

COUNTRY_COLLECTION = Collection(name='country')
COMPANY_COLLECTION = Collection(name='company')

LOCATED_AT = EdgeCollection(from_collection=COMPANY_COLLECTION, name='located_at', to_collection=COUNTRY_COLLECTION)
SUBSIDIARY_OF = EdgeCollection(from_collection=COMPANY_COLLECTION, name='subsidiary_of',
                               to_collection=COUNTRY_COLLECTION)

COLLECTION_NAME_TO_COLLECTION = {
    'country': COUNTRY_COLLECTION,
    'company': COMPANY_COLLECTION,
    'located_at': LOCATED_AT,
    'subsidiary_of': SUBSIDIARY_OF
}


class Company(Document):

    def __init__(self,
                 name: str = None,
                 employee_number: int = None,
                 industry: str = None,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.name = name
        self.industry = industry
        self.employee_number = employee_number

    @classmethod
    def get_collection(cls) -> Collection:
        return COMPANY_COLLECTION


COMPANY_COLLECTION.document_type = Company


class LocatedIn(Edge):

    def __init__(self,
                 since: datetime,
                 until: datetime,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.since = since
        self.until = until

    @classmethod
    def get_collection(cls) -> Collection:
        return LOCATED_AT


LOCATED_AT.document_type = LocatedIn


class SubsidiaryOf(Edge):
    def __init__(self,
                 since: datetime,
                 until: datetime,
                 **kwargs
                 ):
        super().__init__(**kwargs)
        self.since = since
        self.until = until

    @classmethod
    def get_collection(cls) -> Collection:
        return SUBSIDIARY_OF


SUBSIDIARY_OF.document_type = SubsidiaryOf


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
    def get_collection(cls) -> Collection:
        return COUNTRY_COLLECTION


COUNTRY_COLLECTION.document_type = Country