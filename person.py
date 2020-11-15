from dataclasses import dataclass
from collection_definitions import PERSON_COLLECTION, WORKS_AT
from document import Document
from utils import classproperty
from aql_filter import Filter, EdgeFilter, EdgeFilterGenerator, AttributeFilter


@dataclass
class PersonFilter(Filter):
    @property
    def works_at(self):
        return EdgeFilterGenerator(WORKS_AT, self, PersonEdgeFilter)

    def by_name(self, value: str = None, like: str = None):
        if value:
            return PersonAttributeFilter(PERSON_COLLECTION, self,
                                         stmt=f'''filter entity.name == '{value}' ''')

        return PersonAttributeFilter(PERSON_COLLECTION, self, stmt=f'''filter entity.name LIKE '{like}' ''')


class PersonEdgeFilter(EdgeFilter, PersonFilter):
    pass


class PersonAttributeFilter(AttributeFilter, PersonFilter):
    pass


class Person(Document, PersonFilter):
    def __init__(self, name: str, **kwargs):
        super().__init__(**kwargs)
        self.name = name

    def get_collection(self):
        return PERSON_COLLECTION

    @classproperty
    def works_at(cls):
        return EdgeFilterGenerator(WORKS_AT, None, PersonEdgeFilter)

    @classmethod
    def by_name(cls, value: str = None, like: str = None) -> Filter:
        if value:
            return PersonAttributeFilter(PERSON_COLLECTION, None, stmt=f'''filter entity.name == '{value}' ''')

        return PersonAttributeFilter(PERSON_COLLECTION, None, stmt=f'''filter entity.name LIKE '{like}' ''')


PERSON_COLLECTION.document_type = Person
