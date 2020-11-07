from dataclasses import dataclass
from document import Document
from aql_filter import Filter, RelationFilter, AttributeFilter


@dataclass
class CompanyFilter(Filter):
    def by_name(self, name):
        return CompanyRelationFilter(Company, self, f'''filter entity.name == '{name}' ''')


class CompanyRelationFilter(RelationFilter, CompanyFilter):
    pass


class CompanyRelationFilter(AttributeFilter, CompanyFilter):
    pass


class Company(Document):
    def __init__(self, name: str, **kwargs):
        super().__init__(**kwargs)
        self.name = name

    @classmethod
    def get_collection_name(cls):
        return 'company'

    def get_collection(self):
        return Company

    @classmethod
    def by_name(cls, name: str):
        return AttributeFilter(cls, None, stmt=f'''filter entity.name == '{name}' ''')
