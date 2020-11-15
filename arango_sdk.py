from datetime import datetime

from arango import ArangoClient
from collection import Collection
from aql_filter import Filter
from document import Document
from company import Company
from person import Person


class DB:

    def __init__(self, username, password):
        self.client = ArangoClient()
        self.db = self.client.db('test', username=username, password=password)

    def ensure_collection(self, collection: Collection):
        if not self.db.has_collection(collection.name):
            return self.db.create_collection(collection.name)

        return self.db[collection.name]

    def _get_query_results(self, filter_item: Filter):
        query, params = filter_item.filter_by()
        return map(filter_item.collection_from.document_type._load, self.db.aql.execute(query, bind_vars=params))

    def get(self, filter_item):
        return next(self._get_query_results(filter_item), None)

    def get_many(self, filter_item):
        return list(self._get_query_results(filter_item))

    def add(self, document: Document):
        cursor = self.ensure_collection(document.get_collection())
        cursor.insert(document._dump())

    def set(self, from_filter, to_filter):
        from_filter = from_filter(None)
        statement = f'''
        let from_entities = ({from_filter.item.filter_by()})
        let to_entities = ({to_filter.filter_by()})

        for from_entity in from_entities
            for to_entity in to_entities
                insert {{_from: from_entity._id, _to: to_entity._id}} INTO {from_filter.edge_collection_name}
        '''
        self.db.aql.execute(statement)


if __name__ == '__main__':
    db = DB(username='root', password='')
    print(Company.by_name('zirra').by_name('zirra').filter_by()[0])
    print(db.get(Company.by_name('zirra').by_name('zirra')))
    #
    # start = datetime.now()
    #

    #
    # db.add(Company(name='zirra'))
    #
    # db.add(Person(name='Feriha Ibriyamova'))
    # db.add(Person(name='Daniel Gazit'))
    #
    # db.set(Person.by_name('Feriha Ibriyamova').works_at, Company.by_name('zirra'))
    # db.set(Person.by_name('Daniel Gazit').works_at, Company.by_name('zirra'))
    #
    #
    #
    # person = db.get(Person.works_at(Company.by_name('zirra')).by_name(like='%Feriha%'))
    #
    # print(person.name)  # Feriha Ibriyamova
    #
    # elapsed = datetime.now() - start
    # print(elapsed)  # 0:00:00.009002 (9 ms)
