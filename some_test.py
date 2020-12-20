from dataclasses import dataclass

from _collection_definition import document
from new_db import DB


@document(
    collection='company',
)
@dataclass
class Company:
    name: str
    employee_number: int
    industry: str


if __name__ == '__main__':
    new_db = DB(username='root', password='', db_name='test')

    result = new_db.get(Company).match(name='zirra')
    print(result.first())

