import json
import itertools
from inspect import isclass
from typing import Dict, Iterable, Any, List, Union, Type, Callable, TypeVar

from arango import ArangoClient
from arango.graph import Graph
from arango.collection import Collection as ArangoCollection, EdgeCollection as ArangoEdgeCollection
from _collection import Collection, EdgeCollection
from _document import Document, Edge
from _stmt import Stmt
from _query import Query

TEdge = TypeVar('TEdge', bound='Edge')
TDocument = TypeVar('TDocument', bound='Document')


class DB:
    def __init__(self, db_name: str, username: str, password: str, graph_name: str = 'main',
                 serializer: Callable[[Any], str] = json.dumps, deserializer: Callable[[str], Any] = json.loads):
        self.client = ArangoClient(serializer=serializer, deserializer=deserializer)
        self.db = self.client.db(db_name, username=username, password=password)
        self.collection_definition = {}
        self.graph = self._ensure_graph(graph_name)

    def with_collections(self, *collections: Type) -> 'DB':
        for document_type in collections:
            if not issubclass(document_type, Document):
                raise TypeError(f'{document_type} is not a document type')

            collection = document_type._get_collection()
            if issubclass(document_type, Edge):
                self._ensure_edge_collection(collection)
            else:
                self._ensure_collection(collection)

            self.collection_definition[collection.name] = collection

        return self

    def _ensure_graph(self, graph_name: str) -> Graph:
        if self.db.has_graph(graph_name):
            return self.db.graph(graph_name)

        return self.db.create_graph(graph_name)

    def _ensure_edge_collection(self, edge_collection: EdgeCollection) -> ArangoEdgeCollection:
        if not self.graph.has_edge_definition(edge_collection.name):
            return self.graph.create_edge_definition(
                edge_collection=edge_collection.name,
                from_vertex_collections=[from_collection.name for from_collection in edge_collection.from_collections],
                to_vertex_collections=[to_collection.name for to_collection in edge_collection.to_collections]
            )

        return self.graph.edge_collection(edge_collection.name)

    def _ensure_collection(self, collection: Collection) -> ArangoCollection:
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

    def add(self, document: TDocument) -> TDocument:
        cursor = self._ensure_collection(document._get_collection())
        result = cursor.insert(document._dump())
        document._set_meta(**result)
        return document

    def update(self, document: TDocument) -> TDocument:
        cursor = self._ensure_collection(document._get_collection())
        result = cursor.update(document._dump())
        del result['_old_rev']
        document._set_meta(**result)
        return document

    def set(self, from_: Union[Query, Document], edge_document: Union[Type, TEdge], to_: Union[Query, Document],
            data: Dict[str, Any] = None):
        if isinstance(from_, Document) and isinstance(to_, Document):
            return self._set_from_objects(from_, to_, edge_document, data)

        self._ensure_edge_collection(edge_document._get_collection())
        bind_vars = {}

        from_stmt = from_._to_stmt(prefix='from_p') if isinstance(from_, Query) else Stmt(f'[{{_id: @from_id}}]',
                                                                                          bind_vars={
                                                                                              'from_id': from_._id})
        from_str, from_bind_vars = from_stmt.expand()
        bind_vars.update(from_bind_vars)
        to_stmt = to_._to_stmt(prefix='to_p') if isinstance(to_, Query) else Stmt(f'[{{_id: @to_id}}]',
                                                                                  bind_vars={'to_id': to_._id})
        to_str, to_bind_vars = to_stmt.expand()
        bind_vars.update(to_bind_vars)

        if isclass(edge_document):
            dict_doc = Document._dump_from_dict(data, Document.INIT_PROPERTIES)
        else:
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

    def _set_from_objects(self, from_, to_, edge_document, data):
        cursor = self._ensure_edge_collection(edge_document._get_collection())

        if isclass(edge_document):
            return cursor.link(from_._id, to_._id, data=Document._dump_from_dict(data, Document.INIT_PROPERTIES))

        edge_document._from = from_._id
        edge_document._to = to_._id

        cursor.insert(edge_document._dump())

