import json
import itertools
from collections import defaultdict
from dataclasses import dataclass, field
from inspect import isclass
from itertools import repeat
from typing import Dict, Iterable, Any, List, Union, Type, Callable, TypeVar

from arango import ArangoClient
from arango.graph import Graph
from arango.collection import Collection as ArangoCollection, EdgeCollection as ArangoEdgeCollection
from _collection import Collection, EdgeCollection
from _document import Document
from _edge_entity import EdgeEntity
from cursor._str_to_type import COLLECTION_NAME_TO_TYPE

TEdge = TypeVar('TEdge', bound='Edge')
TDocument = TypeVar('TDocument', bound='Document')



class DB:
    def __init__(self, db_name: str, username: str, password: str, graph_name: str = 'main',
                 serializer: Callable[[Any], str] = json.dumps, deserializer: Callable[[str], Any] = json.loads):
        self.client = ArangoClient(serializer=serializer, deserializer=deserializer)
        self.db = self.client.db(db_name, username=username, password=password)
        self.collection_definition = {}
        self.graph = self._ensure_graph(graph_name)
        self._ensure_collections()

    def _ensure_collections(self) -> 'DB':
        for collection_name, document_type in COLLECTION_NAME_TO_TYPE.items():
            if not issubclass(document_type, Document) and not issubclass(document_type, EdgeEntity):
                raise TypeError(f'{document_type} is not a document type')

            collection = document_type._get_collection()
            if issubclass(document_type, EdgeEntity):
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
        return document._get_cursor(db=self)

    def _load_edge(self, collection: str, edge: Dict[str, Any]):
        self.graph.insert_edge(collection=collection, edge=edge)

    def _load_document(self, collection: str, document: Dict[str, Any]):
        self.db.insert_document(collection=collection, document=document)

    def _upsert_edge(self, collection: str, edge: Dict[str, Any]):
        if self.graph.has_edge(edge):
            return self.graph.update_edge(edge)

        self.graph.insert_edge(collection=collection, edge=edge)

    def _upsert_document(self, collection: str, document: Dict[str, Any]):
        if self.db.has_document(document):
            return self.db.update_document(document)

        self.db.insert_document(collection=collection, document=document)

    def upsert(self, document: TDocument) -> TDocument:
        documents = document._dump()

        for is_edge, collection, document in documents:
            if is_edge:
                self._upsert_edge(collection, document)
                continue

            self._upsert_document(collection, document)
