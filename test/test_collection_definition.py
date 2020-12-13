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

    company = Company(name='name', employee_number=30, industry='fin')
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
    assert col.to_collections == [Collection(name='company', document_t

                                  ype=Company)]
