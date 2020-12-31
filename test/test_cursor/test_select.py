from _query import *
from test.test_cursor.test_classes import Company
from test.test_cursor.test_utility import compare_query


def test_select_document():
    compare_query(
        query=Company.match().select(object),
        query_str='''FOR o_p_0 IN company'''
                  '''  RETURN {'''
                  '''    @p_2: (o_p_0)'''
                  '''  }''',
        bind_vars={'p_2': 'document'},
        returns=None,
        result=DictResult(display_name_to_result={'document': DOCUMENT_RESULT})
    )
