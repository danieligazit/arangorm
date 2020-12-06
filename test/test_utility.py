from typing import Dict, Any
from query import Result, Query


def compare_query(query: Query, query_str: str, bind_vars: Dict[str, Any], returns: str, result: Result):
    compare_to_stmt = query._to_stmt()
    compare_to_str, compare_to_bind_vars = compare_to_stmt.expand()
    print(compare_to_str.strip().replace('\n', '').replace(' ', ''))
    print(query_str.strip().replace('\n', '').replace(' ',''))
    assert compare_to_str.strip().replace('\n', '').replace(' ', '') == query_str.strip().replace('\n', '').replace(' ',
                                                                                                                    '')
    assert compare_to_bind_vars == bind_vars
    assert compare_to_stmt.returns == returns
    assert compare_to_stmt.result == result
