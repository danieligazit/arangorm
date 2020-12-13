from datetime import datetime
from dataclasses import dataclass
from _collection_definition import collection, edge_collection


@collection('company')
@dataclass
class Company:
    name: str
    employee_number: int
    industry: str


@collection('country')
@dataclass
class Country:
    name: str
    abbreviation: str


@edge_collection('located_at', from_collections=[Company], to_collections=[Country])
@dataclass
class LocatedIn:
    since: datetime
    until: datetime


@edge_collection('subsidiary_of', from_collections=[Company], to_collections=[Company])
@dataclass
class SubsidiaryOf:
    since: datetime
    until: datetime
