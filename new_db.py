import json
import itertools
from dataclasses import dataclass
from inspect import isclass
from typing import Dict, Iterable, Any, List, Union, Type, Callable, TypeVar

from arango import ArangoClient
from arango.db import Database
from arango.graph import Graph
from arango.collection import Collection as ArangoCollection, EdgeCollection as ArangoEdgeCollection
from _collection import Collection, EdgeCollection
from _document import Document, Edge
from _stmt import Stmt
from _query import Query, eq, AttributeFilter

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

    def get(self, document) -> Any:
        return Cursor(document._get_stmt(prefix='p', max_recursion=document._get_max_recursive()))


@dataclass
class Cursor:
    query: Query
    db: Database
    _matchers: List[AttributeFilter]

    def all(self):
        query_stmt = query._to_stmt()
        query_str, bind_vars = query_stmt.expand()

        return map(self.query._load, self.db.aql.execute(query_str, bind_vars=bind_vars))

    def first(self):
        return next(self.db.aql.execute(self.query), None)

    def match(self, **key_value_match):
        for key, value in key_value_match.items():
            self._matchers.append(eq(key, value))

        return self
