from datetime import datetime
from typing import List, Union, Dict

from arango import ArangoClient
from collection import Collection, EdgeCollection
from aql_filter import Filter, EdgeFilterGenerator
from document import Document, Edge

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
        query =
        load_function = document_type._load

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
        result = cursor.insert(document._dump())
        document._set_meta(**result)
        return document

    def set(self, from_filter: Union[Filter, Document], to_filter: Union[Filter, Document], edge_document: Union[Dict, Edge] = None):
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

#
# if __name__ == '__main__':
#     db = DB(username='root', password='')
#
#     # israel = db.get(Country.by_abbreviation('IL'))
#
#     # us = db.add(Country(name='United States of America', abbreviation='US'))
#
#     # db.set(zirra.located_at, us, LocatedAt(**{'since': datetime.now(), 'until': datetime.now()}))
#
#     query = Company.by_name('zirra').located_at(
#         Country.by_name(like='%America')
#     ).located_at.by_since(value_not='some')
#
#     print(query.filter_by())
#     print(db.get(query))


    # db.set()
    # db.add(Country(name='Italy', abbreviation='IT'))

    # db.set(Company.by_name('zirra').located_at, Country.by_name('Israel'))
    #
    # db.get_many(Company.by_name('zirra').located_at)
    #
    # start = datetime.now()
    #
