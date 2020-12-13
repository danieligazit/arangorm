import itertools
from typing import Dict, Iterable, Any, List, Union

from arango import ArangoClient
from arango.graph import Graph
from arango.collection import Collection as ArangoCollection, EdgeCollection as ArangoEdgeCollection

from _collection import Collection, EdgeCollection
from _document import Document, Edge
from _query import Query, var, out
from _stmt import Stmt
from test.test_classes import Company, LocatedIn, Country, SubsidiaryOf


class DB:
    def __init__(self, db_name: str, username: str, password: str, graph_name: str = 'main',
                 collection_definition: Dict[str, Collection] = None):
        self.client = ArangoClient()
        self.db = self.client.db(db_name, username=username, password=password)
        self.graph = self.ensure_graph(graph_name)
        self.collection_definition = collection_definition

    def with_collection_definition(self, collection_definition: Dict[str, Collection]) -> 'DB':
        self.collection_definition = collection_definition
        return self

    def ensure_graph(self, graph_name: str) -> Graph:
        if self.db.has_graph(graph_name):
            return self.db.graph(graph_name)

        return self.db.create_graph(graph_name)

    def ensure_edge_collection(self, edge_collection: EdgeCollection, from_collections: List[Collection],
                               to_collections: List[Collection]) -> ArangoEdgeCollection:
        if not self.graph.has_edge_definition(edge_collection.name):
            return self.graph.create_edge_definition(
                edge_collection=edge_collection.name,
                from_vertex_collections=[from_collection.name for from_collection in from_collections],
                to_vertex_collections=[to_collection.name for to_collection in to_collections]
            )

        return self.graph.edge_collection(edge_collection.name)

    def ensure_collection(self, collection: Collection) -> ArangoCollection:
        if not self.db.has_collection(collection.name):
            return self.db.create_collection(collection.name)

        return self.db[collection.name]

    def _get_query_results(self, query: Query) -> Iterable:
        query_stmt = query._to_stmt()
        query_str, bind_vars = query_stmt.expand()
        return map(query_stmt.result._load, self.db.aql.execute(query_str, bind_vars=bind_vars),
                   itertools.repeat(self.collection_definition))

    def get(self, query: Query) -> Any:
        return next(self._get_query_results(query), None)

    def get_many(self, query: Query) -> List[Any]:
        return list(self._get_query_results(query))

    def add(self, document: Document):
        cursor = self.ensure_collection(document._get_collection())
        result = cursor.insert(document._dump())
        document._set_meta(**result)
        return document

    def set(self, from_: Union[Query, Document], edge_document: Union[Dict, Edge], to_: Union[Query, Document]):
        if isinstance(from_, Document) and isinstance(to_, Document):
            return self._set_from_objects(from_, to_, edge_document)

        self.ensure_edge_collection(edge_document._get_collection(), [], [])
        bind_vars = {}

        from_stmt = from_._to_stmt() if isinstance(from_, Query) else Stmt(f'[{{_id: @from_id}}]', bind_vars={'from_id': from_._id})
        from_str, from_bind_vars = from_stmt.expand()
        bind_vars.update(from_bind_vars)
        to_stmt = to_._to_stmt() if isinstance(to_, Query) else Stmt(f'[{{_id: @to_id}}]', bind_vars={'to_id': to_._id})
        to_str, to_bind_vars = to_stmt.expand()
        bind_vars.update(to_bind_vars)

        dict_doc = edge_document._dump()
        for key, value in dict_doc.items():
            bind_vars[f'edge_{key}'] = value

        statement = f'''
        LET from_entities = ({from_str})
        LET to_entities = ({to_str})
        FOR from_entity IN from_entities
            FOR to_entity IN to_entities
                insert {{_from: from_entity._id, _to: to_entity._id{',' if len(dict_doc) > 0 else ''} {', '.join([f'{key}: @edge_{key}' for key in dict_doc.keys()])}}} INTO {edge_document._get_collection().name}
        '''
        self.db.aql.execute(statement, bind_vars=bind_vars)

    def _set_from_objects(self, from_, to_, edge_document):
        cursor = self.ensure_edge_collection(edge_document._get_collection(), from_._get_collection(), to_._get_collection())

        if isinstance(edge_document, dict):
            return cursor.link(from_._id, to_._id, data=Document._dump_from_dict(edge_document))

        edge_document._from = from_.item._id
        edge_document._to = to_._id

        cursor.insert(edge_document._dump())


if __name__ == '__main__':
    db = DB('test', username='root', password='').with_collection_definition(COLLECTION_DEFINITION)

    # print(Company.match().out(Company)._to_stmt().expand()[0])
    print(db.get_many(Company.match().out(LocatedIn).to(Country)))