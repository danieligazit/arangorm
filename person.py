from dataclasses import dataclass
from document import Document, Collection
from company import Company
from utils import classproperty
from aql_filter import Filter, RelationFilter, RelationFilterGenerator, AttributeFilter
from relation import Relation


@dataclass
class PersonFilter(Filter):
    @property
    def works_at(self):
        return RelationFilterGenerator(Relation(Person, 'works_at', Company), self, PersonRelationFilter)

    def by_name(self, value: str = None, like: str = None):
        if value:
            return PersonAttributeFilter(Person, self, stmt=f'''filter entity.name == '{value}' ''')

        return PersonAttributeFilter(Person, self, stmt=f'''filter entity.name LIKE '{like}' ''')


class PersonRelationFilter(RelationFilter, PersonFilter):
    pass


class PersonAttributeFilter(AttributeFilter, PersonFilter):
    pass


class Person(Document, PersonFilter):
    def __init__(self, name: str, **kwargs):
        super().__init__(**kwargs)
        self.name = name

    @classmethod
    def get_collection_name(cls):
        return 'person'

    def get_collection(self):
        return Person

    @classproperty
    def works_at(cls):
        return RelationFilterGenerator(Relation(Person, 'works_at', Company), None, PersonRelationFilter)

    @classmethod
    def by_name(cls, value: str = None, like: str = None) -> Filter:
        if value:
            return PersonAttributeFilter(Person, None, stmt=f'''filter entity.name == '{value}' ''')

        return PersonAttributeFilter(Person, None, stmt=f'''filter entity.name LIKE '{like}' ''')
