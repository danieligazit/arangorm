from datetime import datetime
from dataclasses import dataclass
from _collection_definition import edge, document


@document(
    collection='company',
    max_recursion={
        'SubsidiaryOf': 0,
        'Company': 2
    }
)
@dataclass
class Company:
    name: str
    employee_number: int
    industry: str


@edge(
    collection='subsidiary_of',
    from_collections=[Company],
    to_collections=[Company]
)
@dataclass
class SubsidiaryOf:
    confirmed: bool


@document(
    collection='country',
)
@dataclass
class Country:
    name: str
    abbreviation: str


@edge(
    collection='located_in',
    from_collections=[Company],
    to_collections=[Country]
)
@dataclass
class LocatedIn:
    confirmed: bool
