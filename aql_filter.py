from dataclasses import dataclass
from typing import Any
from document import Collection
from relation import Relation


@dataclass
class Filter:
    collection_from: Collection
    item: Any


@dataclass
class RelationFilterGenerator:
    relation: Relation
    item: Any
    relation_filter: Any

    def __call__(self, outbound_filter):
        return self.relation_filter(
            collection_from=self.relation.collection_from,
            item=self.item,
            outbound_item=outbound_filter,
            edge_collection_name=self.relation.collection_name
        )


@dataclass
class RelationFilter(Filter):
    outbound_item: Any
    edge_collection_name: str

    def filter_by(self):
        if self.item is None:
            return f'''
            let outbound_entities = ({self.outbound_item.filter_by()})
            for outbound_entity in outbound_entities
                for result in 1..1 INBOUND outbound_entity._id {self.edge_collection_name}
                    return result
            '''

        return f'''
            let results_a = ({self.item.filter_by()})

            let outbound_entities = ({self.outbound_item.filter_by()})
            for outbound_entity in outbound_entities
                for result in 1..1 INBOUND outbound_entity._id {self.edge_collection_name}
                    filter result in results_a
                    return result
            '''


@dataclass
class AttributeFilter(Filter):
    stmt: str

    def filter_by(self):
        collection_name = self.collection_from.get_collection_name()
        if not self.item:
            return f'''for entity in {collection_name} {self.stmt} return entity'''

        previous_result = f'{collection_name}_entities'
        return f'''let {previous_result} = ({self.item.filter_by()})
        for entity in {previous_result}
        {self.stmt}
        return entity'''
