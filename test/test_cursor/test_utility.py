import re
from typing import Dict, Any, Union

from _document import Document
from _edge_entity import EdgeEntity
from _result import Result
from cursor._cursor import Cursor

_RE_COMBINE_WHITESPACE = re.compile(r"(\s\s)+")


class TestDB:
    def get(self, document):
        return document._get_cursor(db=self)


TEST_DB = TestDB()


def compare_query(cursor: Cursor, query_str: str, bind_vars: Dict[str, Any], returns: str, result: Union[Result, Document, EdgeEntity]):
    compare_to_stmt = cursor._to_stmt(prefix='p')
    compare_to_str, compare_to_bind_vars = compare_to_stmt.expand()
    formatted = compare_to_str
    for key, value in bind_vars.items():
        formatted.replace(key, str(value).replace('True', 'true').replace('False', 'false'))

    formatted = _RE_COMBINE_WHITESPACE.sub("\n", formatted)
    print(formatted)

    print(compare_to_str.strip().replace('\n', '').replace(' ', ''))
    print(query_str.strip().replace('\n', '').replace(' ', ''))
    print(compare_to_stmt.result)
    print(compare_to_bind_vars)

    assert compare_to_str.strip().replace('\n', '').replace(' ', '') == query_str.strip().replace('\n', '').replace(' ',                                                                                                                '')
    assert compare_to_bind_vars == bind_vars
    assert compare_to_stmt.returns == returns
    assert compare_to_stmt.result == result
