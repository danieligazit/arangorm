from collections import defaultdict
from dataclasses import dataclass, field
from typing import Type, Dict, List, Optional

from _collection import EdgeCollection, Collection
from _direction import Direction
from _result import ListResult, AnyResult, Result, DOCUMENT_RESULT
from _stmt import Stmt
from cursor._cursor import Cursor
from cursor._edge_cursor import EdgeCursor
from cursor.filters._filter import Filter


@dataclass
class InnerCursor(Cursor):
    outer_cursor_returns: Optional[str]

    def _set_outer_query(self, outer_query: Cursor):
        if not self.outer_cursor:
            self.outer_cursor = outer_query
            return

        if not isinstance(self.outer_cursor, InnerCursor):
            raise TypeError

        self.outer_cursor._set_outer_query(outer_query)


def traversal_edge_collection_names(edge_collections: List[EdgeCollection]) -> str:
    return ",".join([e.name for e in edge_collections]) if edge_collections else ""


@dataclass
class EdgeTargetTraversalCursor(Filter, InnerCursor):  # , Grouped,
    target_collections: List[Collection]
    outer_cursor: Optional[Cursor]
    direction: Direction

    def out(self, *edge_collection_types: Type['Edge'], min_depth: int = None,
            max_depth: int = None) -> 'EdgeTraversalQuery':
        return EdgeTraversalCursor(
            project=self.project,
            db=self.db,
            outer_cursor_returns='',
            edge_collections=[e._get_collection() for e in edge_collection_types],
            direction=Direction.OUTBOUND,
            outer_cursor=self,
            min_depth=min_depth,
            max_depth=max_depth
        )

    def inbound(self, *edge_collection_types: Type['Edge'], min_depth: int = None,
                max_depth: int = None) -> 'EdgeTraversalCursor':
        return EdgeTraversalCursor(
            outer_query_returns='',
            edge_collections=[e._get_collection() for e in edge_collection_types],
            direction='INBOUND',
            outer_query=self,
            min_depth=min_depth,
            max_depth=max_depth
        )

    def connected_by(self, *edge_collection_types: Type['Edge'], min_depth: int = None, max_depth: int = None):
        return EdgeQuery(
            outer_query_returns='',
            edge_collections=[e._get_collection() for e in edge_collection_types],
            direction='ANY',
            outer_query=self,
            min_depth=min_depth,
            max_depth=max_depth
        )

    def _to_stmt(self, prefix: str = 'p', alias_to_result: Dict[str, Result] = None) -> Stmt:
        return self._get_traversal_stmt(prefix, f'{prefix}_v', alias_to_result)

    def _get_traversal_stmt(self, prefix: str, relative_to: str, alias_to_result: Dict[str, Result] = None):
        if self.outer_cursor_returns:
            relative_to = self.outer_cursor_returns

        if not alias_to_result:
            alias_to_result = {}

        returns = f'{prefix}_v'
        result = self._get_result(AnyResult([t.document_type for t in self.target_collections]) if len(
            self.target_collections) > 0 else DOCUMENT_RESULT)

        step_stmts, bind_vars, bind_vars_index = self._get_step_stmts(relative_to=returns,
                                                                      returns=returns + self.attribute_return,
                                                                      prefix=prefix, bind_vars_index=1)

        filter_target_collection = f'''FILTER {" OR ".join([f"IS_SAME_COLLECTION('{t.name}', {returns})" for t in self.target_collections])}''' if self.target_collections else ''

        if self.outer_cursor:

            edge_query = self.outer_cursor

            previous_str = ''

            outer_query_step_stmts, bind_vars, bind_vars_index = self.outer_cursor._get_step_stmts(
                relative_to=f'{prefix}_e', bind_vars=bind_vars, bind_vars_index=bind_vars_index,
                prefix=f'{prefix}', returns=f'{prefix}_e' + self.outer_cursor.attribute_return)

            if self.outer_cursor.outer_cursor:
                previous_stmt = self.outer_cursor.outer_cursor._to_stmt(f'{prefix}_0', alias_to_result=alias_to_result)
                alias_to_result.update(previous_stmt.alias_to_result)
                previous_str, previous_bind_vars = previous_stmt.expand_without_return()
                bind_vars.update(previous_bind_vars)
                relative_to = previous_stmt.returns

            return Stmt(f'''
            {previous_str}
            FOR {prefix}_v, {prefix}_e IN {self.outer_cursor.min_depth}..{self.outer_cursor.max_depth} {edge_query.direction} {relative_to}._id {",".join([e.name for e in edge_query.edge_collections]) if edge_query.edge_collections else ""}
                {filter_target_collection}
                {step_stmts}
                {outer_query_step_stmts}
            ''', bind_vars, alias_to_result=alias_to_result, returns=returns + self.attribute_return,
                        result=result, aliases=self.aliases)

        return Stmt(f'''
            {filter_target_collection}
            {step_stmts}
        ''', bind_vars, alias_to_result=alias_to_result, result=result,
                    returns=returns + self.attribute_return, aliases=self.aliases)

    def _to_filter_stmt(self, prefix: str = 'p', relative_to: str = None) -> Stmt:

        relative_to = relative_to[:-1] + 'v' if relative_to.endswith('e') else relative_to
        returns = f'{prefix}_v'

        if self.outer_cursor:
            edge_query = self.outer_cursor
            filter_target_collection = f'''FILTER {" OR ".join([f"IS_SAME_COLLECTION('{t.name}', {returns})" for t in self.target_collections])}''' if self.target_collections else ''
            previous_str = ''

            if self.outer_cursor.outer_query:
                filter_target_collection = f'''FILTER {" OR ".join([f"IS_SAME_COLLECTION('{t.name}', {relative_to})" for t in self.target_collections])}''' if self.target_collections else ''

                step_stmts, bind_vars, bind_vars_index = self._get_step_stmts(relative_to=relative_to,
                                                                              returns=returns + self.attribute_return,
                                                                              prefix=prefix, bind_vars_index=0)

                previous_stmt = self.outer_cursor.outer_query._to_stmt(f'{prefix}_0')
                previous_str, previous_bind_vars = previous_stmt.expand_without_return()
                bind_vars.update(previous_bind_vars)
                relative_to = previous_stmt.returns

            else:
                step_stmts, bind_vars, bind_vars_index = self._get_step_stmts(relative_to=returns,
                                                                              returns=returns + self.attribute_return,
                                                                              prefix=prefix, bind_vars_index=0)

            outer_query_step_stmts, bind_vars, bind_vars_index = self.outer_cursor._get_step_stmts(
                relative_to=f'{prefix}_e', bind_vars=bind_vars, bind_vars_index=bind_vars_index,
                prefix=f'{prefix}', returns=f'{prefix}_e' + self.outer_cursor.attribute_return)

            return Stmt(f'''
            LET {prefix}_sub = (
                {previous_str}
                FOR {prefix}_v, {prefix}_e IN {self.outer_cursor.min_depth}..{self.outer_cursor.max_depth} {edge_query.direction} {relative_to}._id {",".join([e.name for e in edge_query.edge_collections]) if edge_query.edge_collections else ""}
                    {filter_target_collection}
                    {step_stmts}
                    {outer_query_step_stmts}
                    RETURN 1
            )
            FILTER LENGTH({prefix}_sub) > 0
            ''', bind_vars)

        filter_target_collection = f'''FILTER {" OR ".join([f"IS_SAME_COLLECTION('{t.name}', {relative_to})" for t in self.target_collections])}''' if self.target_collections else ''

        step_stmts, bind_vars, bind_vars_index = self._get_step_stmts(relative_to=relative_to,
                                                                      returns=returns + self.attribute_return,
                                                                      prefix=prefix, bind_vars_index=0)

        return Stmt(f'''
            LET {prefix}_sub = (
                {filter_target_collection}
                {step_stmts}
                RETURN 1
            )
            FILTER LENGTH({prefix}_sub) > 0

        ''', bind_vars)

    def _to_group_stmt(self, prefix: str, collected, alias_to_result: Dict[str, Result]) -> Stmt:
        traversal_stmt = self._get_traversal_stmt(prefix, relative_to=f'{prefix}_doc', alias_to_result=alias_to_result)

        traversal_stmt.query_str = f'''
                    FOR {prefix}_doc in {collected}[*]
                        {traversal_stmt.query_str}
                '''

        traversal_stmt.result = ListResult(AnyResult([e.document_type for e in self.target_collections]))

        return traversal_stmt


@dataclass
class EdgeTraversalCursor(Filter, InnerCursor):  # , Grouped, Selected,
    edge_collections: List[EdgeCollection]
    outer_cursor: Optional[Cursor]
    direction: Direction
    min_depth: int
    max_depth: int

    def __post_init__(self):
        if self.max_depth is None:
            if self.min_depth is None:
                self.min_depth, self.max_depth = 1, 1
                return
            self.max_depth = self.min_depth
            return

        if self.min_depth is None:
            self.min_depth = 1

    def to(self, *target_collection_types: Type['Document']):
        return EdgeTargetTraversalCursor(
            project=None,
            db=self.db,
            outer_cursor_returns='',
            outer_cursor=self,
            target_collections=[t._get_collection() for t in target_collection_types],
            direction=self.direction,
        )

    def _get_traversal_stmt(self, prefix: str, relative_to: str = '', alias_to_result: Dict[str, Result] = None):
        if not alias_to_result:
            alias_to_result = {}

        result = self._get_result(DOCUMENT_RESULT)
        step_stmts, bind_vars, bind_vars_index = self._get_step_stmts(relative_to=f'{prefix}_e',
                                                                      returns=f'{prefix}_e' + self.attribute_return,
                                                                      prefix=prefix)

        if self.outer_cursor:
            previous_stmt = self.outer_cursor._to_stmt(prefix=f'{prefix}_0', alias_to_result=alias_to_result)
            alias_to_result.update(previous_stmt.alias_to_result)
            previous_str, previous_vars = previous_stmt.expand_without_return()
            bind_vars.update(previous_vars)

            return Stmt(f'''
                {previous_str}
                    FOR {prefix}_v, {prefix}_e IN {self.min_depth}..{self.max_depth} {self.direction} {previous_stmt.returns}._id {traversal_edge_collection_names(self.edge_collections)}
                        {step_stmts}
                ''', bind_vars, returns=f'{prefix}_e' + self.attribute_return, result=result, aliases=self.aliases,
                        alias_to_result=alias_to_result)

        return Stmt(f'''
            FOR {prefix}_v, {prefix}_e IN {self.min_depth}..{self.max_depth} {self.direction} {relative_to}._id {traversal_edge_collection_names(self.edge_collections)}
                {step_stmts}
        ''', bind_vars, returns=f'{prefix}_e' + self.attribute_return, result=result, aliases=self.aliases)

    def _to_stmt(self, prefix: str = 'p', alias_to_result: Dict[str, Result] = None) -> Stmt:
        return self._get_traversal_stmt(prefix, alias_to_result=alias_to_result, relative_to=self.outer_cursor_returns)

    def _to_filter_stmt(self, prefix: str = 'p', relative_to: str = None) -> Stmt:
        traversal_stmt = self._get_traversal_stmt(prefix, relative_to=relative_to)
        traversal_stmt.query_str = f'''
            LET {prefix}_sub = (
                {traversal_stmt.query_str}
                RETURN 1
            )

            FILTER LENGTH({prefix}_sub) > 0'''
        return traversal_stmt

    def _to_group_stmt(self, prefix: str, collected: str, alias_to_result: Dict[str, Result]) -> Stmt:
        traversal_stmt = self._get_traversal_stmt(prefix, relative_to=f'{prefix}_doc', alias_to_result=alias_to_result)

        traversal_stmt.query_str = f'''
            FOR {prefix}_doc in {collected}[*]
                {traversal_stmt.query_str}
        '''

        traversal_stmt.result = ListResult(AnyResult([e.document_type for e in self.edge_collections]))

        return traversal_stmt

    def _to_select_stmt(self, prefix: str, relative_to: str, alias_to_result: Dict[str, Result]) -> Stmt:
        traversal_stmt = self._get_traversal_stmt(prefix, relative_to=relative_to, alias_to_result=alias_to_result)
        traversal_stmt.result = AnyResult([e.document_type for e in self.edge_collections])
        return traversal_stmt


@dataclass
class DocumentCursor(Cursor):
    collection: Collection

    def _to_stmt(self, prefix: str, alias_to_result: Dict[str, Result] = None):
        if not alias_to_result:
            alias_to_result = {}

        returns = f'o_{prefix}'
        step_str, bind_vars, _ = self._get_step_stmts(prefix=prefix, relative_to=returns,
                                                      returns=returns)

        return Stmt(f'''
            FOR {returns} IN {self.collection.name}
                {step_str}
            ''', returns=returns, bind_vars=bind_vars, result=self._get_result(self.collection.document_type),
                    aliases=self.aliases, alias_to_result=alias_to_result)

    def outbound(self, *edge_collection_types: Type['Edge'], min_depth: int = None,
                 max_depth: int = None) -> EdgeTraversalCursor:
        return EdgeTraversalCursor(
            db=self.db,
            project=None,
            outer_cursor_returns='',
            edge_collections=[e._get_collection() for e in edge_collection_types],
            direction=Direction.OUTBOUND,
            outer_cursor=self,
            min_depth=min_depth,
            max_depth=max_depth
        )


def outbound(*edge_collection_types: Type['Edge'], min_depth: int = None, max_depth: int = None) -> EdgeTraversalCursor:
    return EdgeTraversalCursor(
        db=None,
        project=None,
        outer_cursor_returns=None,
        edge_collections=[e._get_collection() for e in edge_collection_types],
        direction=Direction.OUTBOUND,
        outer_cursor=None,
        min_depth=min_depth,
        max_depth=max_depth
    )


def inbound(*edge_collection_types: Type['Edge'], min_depth: int = None, max_depth: int = None) -> EdgeTraversalCursor:
    return EdgeTraversalCursor(
        db=None,
        project=None,
        outer_cursor_returns=None,
        edge_collections=[e._get_collection() for e in edge_collection_types],
        direction=Direction.INBOUND,
        outer_cursor=None,
        min_depth=min_depth,
        max_depth=max_depth
    )


def connected_by(*edge_collection_types: Type['Edge'], min_depth: int = None,
                 max_depth: int = None) -> EdgeTraversalCursor:
    return EdgeTraversalCursor(
        db=None,
        project=None,
        outer_cursor_returns='',
        edge_collections=[e._get_collection() for e in edge_collection_types],
        direction=Direction.ANY,
        outer_cursor=None,
        min_depth=min_depth,
        max_depth=max_depth
    )


def to(*collection_types: Type['Document']) -> EdgeTargetTraversalCursor:
    return EdgeTargetTraversalCursor(
        target_collections=[c._get_collection() for c in collection_types],
        outer_cursor=None,
        outer_cursor_returns=None,
        direction=Direction.OUTBOUND
    )
