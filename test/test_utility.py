import re
from typing import Dict, Any

from query import Result, Query

_RE_COMBINE_WHITESPACE = re.compile(r"(\s\s)+")


def compare_query(query: Query, query_str: str, bind_vars: Dict[str, Any], returns: str, result: Result):
    compare_to_stmt = query._to_stmt()
    compare_to_str, compare_to_bind_vars = compare_to_stmt.expand()

    formatted = compare_to_str
    for key, value in bind_vars.items():
        formatted.replace(key, str(value).replace('True', 'true').replace('False', 'false'))

    formatted = _RE_COMBINE_WHITESPACE.sub("\n", formatted)
    print(formatted)

    print(compare_to_str.strip().replace('\n', '').replace(' ', ''))
    print(query_str.strip().replace('\n', '').replace(' ', ''))
    print(compare_to_stmt.result)

    # assert compare_to_str.strip().replace('\n', '').replace(' ', '') == query_str.strip().replace('\n', '').replace(' ',                                                                                                                '')
    # assert compare_to_bind_vars == bind_vars
    # assert compare_to_stmt.returns == returns
    # assert compare_to_stmt.result == result
