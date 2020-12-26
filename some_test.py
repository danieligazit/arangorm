from dataclasses import dataclass

from _collection_definition import document, edge
from _direction import Direction
from cursor.project._project import HasEdge, EdgeTarget
from new_db import DB


@document(
    collection='company',
    edge_schema={
        'subsidiary_of': HasEdge('SubsidiaryOf', direction=Direction.OUTBOUND),
        'has_subsidiaries': HasEdge('SubsidiaryOf', direction=Direction.INBOUND, many=True)
    },
    max_recursion={
        'SubsidiaryOf': 2,
        'Company': 2
    }
)
@dataclass
class Company:
    name: str
    employee_number: int
    industry: str
    subsidiary_of: 'SubsidiaryOf'
    has_subsidiaries: 'SubsidiaryOf'


@edge(
    collection='subsidiary_of',
    target_schema={
        'parent': EdgeTarget('Company', _to=True),
        'daughter': EdgeTarget('Company', _to=False)
    }
)
@dataclass
class SubsidiaryOf:
    confirmed: bool
    parent: Company
    daughter: Company


if __name__ == '__main__':
    opm = DB(username='root', password='', db_name='test')
    zirra = opm.get(Company).match(employee_number=30).first()
    zirra.subsidiary_of.daughter.name = 'zirra'
    opm.upsert(zirra)
    zirra = opm.get(Company).match(employee_number=30).first()
    print(zirra)