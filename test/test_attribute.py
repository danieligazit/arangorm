from test.test_classes import Country
from query import *
from test.test_classes import Company, LocatedIn
from test.test_utility import compare_query


def test_document_attribute():
    compare_query(
        query=Company.match().name,
        query_str='''FOR o_p IN company'''
                  '''  RETURN o_p.name''',
        bind_vars={},
        returns='o_p.name',
        result=VALUE_RESULT,
    )


def test_document_as_var_select_attribute():
    compare_query(
        query=Company.match().as_var('a').select(var('a').name),
        query_str='''FOR o_p_0 IN company'''
                  '''  LET a = o_p_0'''
                  '''  RETURN {'''
                  '''    @p_2: (a.name)'''
                  '''  }''',
        bind_vars={'p_2': 'a'},
        returns=None,
        result=DictResult(display_name_to_result={'a': VALUE_RESULT}),
    )
