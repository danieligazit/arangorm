from datetime import datetime
from typing import List

from arango import ArangoClient
from collection import Collection, EdgeCollection
from aql_filter import Filter, EdgeFilterGenerator
from document import Document

from model import Country, Company, LocatedAt


class DB:
    def __init__(self, username: str, password: str, graph_name: str = 'main'):
        self.client = ArangoClient()
        self.db = self.client.db('test', username=username, password=password)
        self.graph = self.ensure_graph('main')

    def ensure_graph(self, graph_name: str):
        if self.db.has_graph(graph_name):
            return self.db.graph(graph_name)

        return self.db.create_graph(graph_name)

    def ensure_edge_collection(self, edge_collection: EdgeCollection, from_collection: Collection, to_collection: Collection):
        if not self.graph.has_edge_definition(edge_collection.name):
            return self.graph.create_edge_definition(
                edge_collection=edge_collection.name,
                from_vertex_collections=[from_collection.name],
                to_vertex_collections=[to_collection.name]
            )

        return self.graph.edge_collection(edge_collection.name)

    def ensure_collection(self, collection: Collection):
        if not self.db.has_collection(collection.name):
            return self.db.create_collection(collection.name)

        return self.db[collection.name]

    def _get_query_results(self, filter_item: Filter):
        query, params = filter_item.filter_by()

        if isinstance(filter_item, EdgeFilterGenerator):
            load_function = filter_item.edge.document_type._load
        else:
            load_function = filter_item.collection_from.document_type._load

        print(query, params )
        return map(load_function, self.db.aql.execute(query, bind_vars=params))

    def count(self, filter_item: Filter):
        query, params = filter_item.filter_by()
        return self.db.aql.execute(query, bind_vars=params, count=True).count()

    def get(self, filter_item):
        return next(self._get_query_results(filter_item), None)

    def get_many(self, filter_item):
        return list(self._get_query_results(filter_item))

    def add(self, document: Document):
        cursor = self.ensure_collection(document.get_collection())
        cursor.insert(document._dump())

    def set(self, from_filter, to_filter, edge_document = None):
        if edge_document and isinstance(from_filter.item, Document) and isinstance(to_filter, Document):
            return self._set_from_objects(from_filter, to_filter, edge_document)

        from_filter = from_filter(None)
        from_stmt, from_params = from_filter.item.filter_by('f')
        to_stmt, to_params = to_filter.filter_by('t')

        statement = f'''
        let from_entities = ({from_stmt})
        let to_entities = ({to_stmt})

        for from_entity in from_entities
            for to_entity in to_entities
                insert {{_from: from_entity._id, _to: to_entity._id}} INTO {from_filter.edge_collection.name}
        '''

        from_params.update(to_params)
        print(statement, from_params)
        self.db.aql.execute(statement, bind_vars=to_params)

    def _set_from_filter(self):
        return

    def _set_from_objects(self, from_filter, to_object, edge_document):
        cursor = self.ensure_edge_collection(from_filter.edge, from_filter.edge.from_collection, to_object.get_collection())

        if isinstance(edge_document, dict):
            return cursor.link(from_filter.item._id, to_object._id, data=Document._dump_from_dict(edge_document))

        edge_document._from = from_filter.item._id
        edge_document._to = to_object._id

        cursor.insert(edge_document._dump())


if __name__ == '__main__':
    db = DB(username='root', password='')

    zirra = db.get(Company.by_name('zirra'))
    italy = db.get(Country.by_abbreviation('IL'))

    # db.set(zirra.located_at, italy, LocatedAt(**{'since': datetime.now(), 'until': datetime.now()}))

    print(db.get(zirra.located_at))


    # db.set()
    # db.add(Country(name='Italy', abbreviation='IT'))

    # db.set(Company.by_name('zirra').located_at, Country.by_name('Israel'))
    #
    # db.get_many(Company.by_name('zirra').located_at)
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
