from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Union, List, Type, Dict
from document import Edge, Document
from collection import Collection, EdgeCollection
import model.collection_definition as col

DELIMITER = '\n'


@dataclass
class EdgeFilter:
    edge_collections: List[EdgeCollection]
    direction: str
    previous_filter: Any
    match_objects: List = field(default_factory=list)

    def match(self, *match_objects, **key_value_match):
        self.match_objects += match_objects

        for key, value in key_value_match.items():
            self.match_objects.append(eq(key, value))

        return self

    def to(self, *target_collection_types: List[Type[Document]]):
        return EdgeFilterTarget(
            edge_filter=self,
            target_collections=[t.get_collection() for t in target_collection_types]
        )

    def get_filter_stmt(self):
        return f'''
        let previous = ({self.previous_filter.get_filter_stmt()})
        for entity in previous
            for v, e in 1..1 {self.direction} entity._id {",".join([e.name for e in self.edge_collections]) if self.edge_collections else ""}
                {DELIMITER.join(map(lambda match_object: match_object.get_filter_stmt(), self.match_objects))}
                return e
        '''



class FilterDocument:
    def __init__(self, collection = None, match_objcets: List = None, key_value_match: Dict[str, Any] = None, previous=None):
        self.collection = collection
        self.match_objects = match_objcets if match_objcets else []
        self.previous = previous

        for key, value in key_value_match:
            self.match_objects.append(eq(key, value))

    def match(self, *match_objects, **key_value_match):
        self.match_objects += match_objects

        for key, value in key_value_match.items():
            self.match_objects.append(eq(key, value))

        return self

    def out(self, *edge_collection_types: List[Type[Collection]]):
        return EdgeFilter(
            edge_collections=[e.get_collection() for e in edge_collection_types],
            direction='outbound',
            previous_filter=self
        )

    def get_filter_stmt(self):
        if self.collection:
            return f'''for entity in {self.collection.name}
            {DELIMITER.join(map(lambda match_object: match_object.get_filter_stmt(), self.match_objects))}
            return entity
            '''

        if self.previous:
            return f'''let previous = ({self.previous.get_filter_stmt()})
            for entity in previous
            {DELIMITER.join(map(lambda match_object: match_object.get_filter_stmt(), self.match_objects))}
            return entity
            '''

        raise ValueError


@dataclass
class EdgeFilterTarget(FilterDocument):
    target_collections: List[Collection]
    edge_filter: 'EdgeFilter'
    match_objects: List = field(default_factory=list)

    def get_filter_stmt(self):
        return f'''
        let previous = ({self.edge_filter.previous_filter.get_filter_stmt()})
        for entity in previous
            for v, e in 1..1 {self.edge_filter.direction} entity._id {",".join([e.name for e in self.edge_filter.edge_collections]) if self.edge_filter.edge_collections else ""}
                filter {" or ".join([f"IS_SAME_COLLECTION('{t.name}', v)" for t in self.target_collections])}
                {DELIMITER.join(map(lambda match_object: match_object.get_filter_stmt(), self.match_objects))}
                return v
        '''

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
    def get_collection(cls) -> Collection:
        return col.COMPANY_COLLECTION

    @classmethod
    def match(cls, *match_objects, **key_value_match):
        return FilterDocument(cls.get_collection(), match_objects, key_value_match)

col.COMPANY_COLLECTION.document_type = Company


class LocatedIn(Edge, FilterDocument):

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
        return col.LOCATED_AT



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
        return col.COUNTRY_COLLECTION

    @classmethod
    def match(cls, *match_objects, **key_value_match):
        return FilterDocument(cls.get_collection(), match_objects, key_value_match)


class AttributeFilter:
    attribute: str
    operator: str
    compare_value: Any

    def __init__(self, attribute: str, operator: str, compare_value: Any):
        super().__init__()
        self.attribute = attribute
        self.operator = operator
        self.compare_value = compare_value

    def get_filter_stmt(self):
        return f'filter entity.{self.attribute} {self.operator} {self.compare_value}'

col.LOCATED_AT.document_type = LocatedIn

@dataclass
class EdgeDocumentFilter:
    edge_collection: EdgeCollection
    edge_filters: List = field(default_factory=list)
    to_filters: List = field(default_factory=list)

    def match(self, *edge_filters, **key_value_match):
        self.edge_filters += edge_filters

        for key, value in key_value_match.items():
            self.edge_filters.append(eq(key, value))

        return self


    def to(self, *to_filter):
        self.to_filters += to_filter
        return self

    def get_filter_stmt(self):
        return f'''let sub = (
            for v, e IN 1..1 OUTBOUND entity._id {self.edge_collection.name}
            {DELIMITER.join(map(lambda match_object: match_object.get_filter_stmt(), self.edge_filters))}
            {DELIMITER.join(self.to_filters)}
            return 1
        )

        filter length(sub1) > 0
        '''


def like(attribute: str, compare_value: Any) -> AttributeFilter:
    return AttributeFilter(attribute=attribute, operator='LIKE', compare_value=compare_value)

def eq(attribute: str, compare_value: Any) -> AttributeFilter:
    return AttributeFilter(attribute=attribute, operator='==', compare_value=compare_value)

def gt(attribute: str, compare_value: Any) -> AttributeFilter:
    return AttributeFilter(attribute=attribute, operator='>', compare_value=compare_value)

def out(edge_collection_type: Type[Document]) -> EdgeDocumentFilter:
    return EdgeDocumentFilter(edge_collection=edge_collection_type.get_collection())


query = Company.match(
    like('name', '%zirra%'),
    gt('employee_number', 5),
    out(LocatedIn).match(since=datetime(2018, 2, 1)).match(some_data_point='yes')
).out(LocatedIn).to(Country)

print(query.get_filter_stmt())


'''
for c in company
    filter c.name like '%zirra%'
    filter c.employee_number > 5

    let sub1 = (
        for v, e IN 1..1 OUTBOUND c._id company
        filter e.since == '2018-02-01'
        filter v.name like '%America%'
    )

    filter length(sub1)

    return c
'''
#.connects('subsidiary_of').to(
#     Company.match(name='zirra')
# ).connected_by('works_at').from_



