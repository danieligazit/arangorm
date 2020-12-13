from dataclasses import dataclass
from datetime import datetime, timedelta

from _collection import Collection
from _collection_definition import collection, edge_collection


def test_class_collection():
    @collection('company')
    class Company:
        def __init__(self, name: str, employee_number: int, industry: str):
            self.name = name
            self.employee_number = employee_number
            self.industry = industry

    company = Company('name', employee_number=30, industry='fin')
    col = company._get_collection()

    assert company.name == 'name'
    assert company.employee_number == 30
    assert company.industry == 'fin'
    assert col.name == 'company'
    assert col.document_type == Company


def test_class_edge_collection():
    @collection('company')
    class Company:
        def __init__(self, name: str, employee_number: int, industry: str):
            self.name = name
            self.employee_number = employee_number
            self.industry = industry

    @edge_collection('subsidiary_of', from_collections=[Company], to_collections=[Company])
    class SubsidiaryOf:
        def __init__(self, since: datetime, until: datetime):
            self.since = since
            self.until = until

    subsidiary_of = SubsidiaryOf(since=datetime.now() - timedelta(days=365), until=datetime.now())
    col = subsidiary_of._get_collection()

    assert subsidiary_of.since == datetime.now() - timedelta(days=365)
    assert subsidiary_of.until == datetime.now()

    assert col.name == 'subsidiary_of'
    assert col.document_type == SubsidiaryOf
    assert col.from_collections == [Collection(name='company', document_type=Company)]
    assert col.to_collections == [Collection(name='company', document_type=Company)]


def test_class_methods():
    @collection('company')
    class Company:
        def __init__(self, name: str, employee_number: int, industry: str):
            self.name = name
            self.employee_number = employee_number
            self.industry = industry

        def get_name(self) -> str:
            return self.name

    @edge_collection('subsidiary_of', from_collections=[Company], to_collections=[Company])
    class SubsidiaryOf:
        def __init__(self, since: datetime, until: datetime):
            self.since = since
            self.until = until

        def set_since(self, new_value: datetime):
            self.since = new_value

    company = Company(name='name', employee_number=30, industry='fin')
    subsidiary_of = SubsidiaryOf(since=datetime.now() - timedelta(days=365), until=datetime.now())

    assert company.get_name() == 'name'
    now = datetime.now()
    subsidiary_of.set_since(now)
    assert subsidiary_of.since == now


def test_class_init_method():
    @collection('company')
    class Company:
        def __init__(self, name: str, employee_number: int = 30, industry: str = 'fin'):
            self.name = name
            self.employee_number = employee_number * 20
            self.industry = industry

    company = Company('name')
    assert company.name == 'name'
    assert company.employee_number == 30 * 20
    assert company.industry == 'fin'


def test_class_edge_init_method():
    @collection('company')
    class Company:
        def __init__(self, name: str, employee_number: int = 30, industry: str = 'fin'):
            self.name = name
            self.employee_number = employee_number * 20
            self.industry = industry

    @edge_collection('subsidiary_of', from_collections=[Company], to_collections=[Company])
    class SubsidiaryOf:
        def __init__(self, years_long: int, until: datetime = datetime(1685, 3, 21)):
            self.since = until - timedelta(days=365 * years_long)
            self.until = until

    subsidiary_of = SubsidiaryOf(1)
    assert subsidiary_of.since == datetime(1684, 3, 21)
    assert subsidiary_of.until == datetime(1685, 3, 21)


def test_dataclass():
    @collection('company')
    @dataclass
    class Company:
        name: str
        employee_number: int
        industry: str

    company = Company('name', 30, 'fin')
    assert company.name == 'name'
    assert company.employee_number == 30
    assert company.industry == 'fin'


def test_dataclass():
    @collection('company')
    @dataclass
    class Company:
        name: str
        employee_number: int
        industry: str

    company = Company('name', 30, 'fin')
    col = company._get_collection()

    assert company.name == 'name'
    assert company.employee_number == 30
    assert company.industry == 'fin'
    assert col.name == 'company'
    assert col.document_type == Company


def test_dataclass_edge():
    @collection('company')
    @dataclass
    class Company:
        name: str
        employee_number: int
        industry: str

    @edge_collection('subsidiary_of', from_collections=[Company], to_collections=[Company])
    @dataclass
    class SubsidiaryOf:
        since: datetime
        until: datetime

    subsidiary_of = SubsidiaryOf(since=datetime.now() - timedelta(days=365), until=datetime.now())
    col = subsidiary_of._get_collection()

    assert subsidiary_of.since == datetime.now() - timedelta(days=365)
    assert subsidiary_of.until == datetime.now()

    assert col.name == 'subsidiary_of'
    assert col.document_type == SubsidiaryOf
    assert col.from_collections == [Collection(name='company', document_type=Company)]
    assert col.to_collections == [Collection(name='company', document_type=Company)]