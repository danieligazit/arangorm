import inspect
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Type, Tuple, Dict, TypeVar, Generic
from collection import Collection, EdgeCollection
from document import Document

@dataclass
class Filter(ABC):
    collection_from: Collection
    item: 'Filter'

    # @abstractmethod
    # def filter_by(self, prefix: str = 'p', depth: int = 0) -> Tuple[str, Dict[str, Any]]:
    #     pass


@dataclass
class EdgeFilter(Filter):
    outbound_item: Any
    edge_collection: EdgeCollection

    def filter_by(self, prefix: str = 'p', depth: int = 0) -> Tuple[str, Dict[str, Any]]:
        outbound_entities_query, outbound_parameters = self.outbound_item.filter_by('l' + prefix, depth+1)

        if self.item is None:
            return f'''
            let outbound_entities = ({outbound_entities_query})
            for outbound_entity in outbound_entities
                for result in 1..1 INBOUND outbound_entity._id {self.edge_collection.name}
                    return result
            ''', outbound_parameters

        results_query, results_parameters = self.item.filter_by(self.item, 'r'+prefix, depth+1)
        results_parameters.update(outbound_parameters)
        return f'''
            let results_a = ({results_query})

            let outbound_entities = ({outbound_entities_query})
            for outbound_entity in outbound_entities
                for result in 1..1 INBOUND outbound_entity._id {self.edge_collection.name}
                    filter result in results_a
                    return result
            ''', results_parameters


@dataclass
class EdgeFilterGenerator():
    edge: EdgeCollection
    item: Filter
    edge_filter: Type[EdgeFilter]

    def __call__(self, outbound_filter):
        return self.edge_filter(
            collection_from=self.edge.from_collection,
            item=self.item,
            outbound_item=outbound_filter,
            edge_collection=self.edge
        )

    def filter_by(self, prefix: str = 'p', depth: int = 0):
        outbound_entities_query, outbound_parameters = self.item.filter_by('l' + prefix, depth+1)

        return f'''
        let outbound_entities = ({outbound_entities_query})
        for outbound_entity in outbound_entities
            for v, result in 1..1 OUTBOUND outbound_entity._id {self.edge.name}
                return result
        ''', outbound_parameters


@dataclass
class AttributeFilter(Filter):
    attribute: str
    operator: str
    compare_value: Any

    def filter_by(self, prefix: str = 'p', depth: int = 0) -> Tuple[str, Dict[str, Any]]:
        collection_name = self.collection_from.name

        compare_attribute = f'{prefix}{depth}'
        params = {
            compare_attribute: self.compare_value
        }

        statement = f'entity.{self.attribute} {self.operator} @{compare_attribute}'
        if not self.item:
            return f'''for entity in {collection_name} filter {statement} return entity''', params

        previous, prev_params = self.item.filter_by(prefix, depth+1)
        params.update(prev_params)
        return f'''let previous = ({previous})
        for entity in previous
        filter {statement}
        return entity''', params


def comparable_attribute_filter(attribute: str):
    def decorator_function(func):
        def inner_function(
                cls,
                value: Any = None,
                value_not: Any = None,
                lt: Any = None,
                lte: Any = None,
                gt: Any = None,
                gte: Any = None,
                value_in: List[Any] = None,
                not_in: List[Any] = None,
        ) -> AttributeFilter:
            for compare_value, operator in [
                (value, '=='),
                (value_not, '!='),
                (lt, '<'),
                (lte, '<='),
                (gt, '>'),
                (gte, '>='),
                (value_in, 'IN'),
                (not_in, 'NOT IN'),
            ]:
                if compare_value:
                    return func(cls)(cls.get_collection(), None if isinstance(cls, Document) else cls, attribute, operator, compare_value)

            raise ValueError('Method not provided with a compare value')

        return inner_function

    return decorator_function


def default_attribute_filter(attribute: str):
    def decorator_function(func):
        def inner_function(
                cls,
                value: Any = None,
                value_not: Any = None,
                value_in: List[Any] = None,
                not_in: List[Any] = None,
        ) -> AttributeFilter:
            for compare_value, operator in [
                (value, '=='),
                (value_not, '!='),
                (value_in, 'IN'),
                (not_in, 'NOT IN'),
            ]:
                if compare_value:
                    return func(cls)(cls.get_collection(), None if isinstance(cls, Document) else cls, attribute, operator, compare_value)

            raise ValueError('Method not provided with a compare value')

        return inner_function

    return decorator_function


def string_attribute_filter(attribute: str):
    def decorator_function(func):
        def inner_function(
                cls,
                value: str = None,
                value_not: str = None,
                value_in: List[str] = None,
                not_in: List[str] = None,
                like: str = None,
                not_like: str = None,
                matches_regex: str = None,
                not_matches_regex: str = None
        ):
            for compare_value, operator in [
                (value, '=='),
                (value_not, '!='),
                (value_in, 'IN'),
                (not_in, 'NOT IN'),
                (like, 'LIKE'),
                (not_like, 'NOT LIKE'),
                (matches_regex, '=~'),
                (not_matches_regex, "!~")
            ]:
                if compare_value:
                    return func(cls)(cls.get_collection(), None if inspect.isclass(cls) else cls, attribute, operator, compare_value)

            raise ValueError('Method not provided with a compare value')

        return inner_function

    return decorator_function

