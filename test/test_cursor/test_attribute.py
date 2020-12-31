from _query import *
from test.test_cursor.test_classes import Company
from test.test_cursor.test_utility import compare_query


def test_document_attribute():
    compare_query(
        query=Company.match().name,
        query_str='''FOR o_p IN company'''
                  '''  RETURN o_p.name''',
        bind_vars={},
        returns='o_p.name',
        result=VALUE_RESULT,
    )


def test_document_nested_attribute():
    compare_query(
        query=Company.match().address.city,
        query_str='''FOR o_p IN company'''
                  '''  RETURN o_p.address.city''',
        bind_vars={},
        returns='o_p.address.city',
        result=VALUE_RESULT,
    )


def test_edge_attribute():
    compare_query(
        query=Company.match().outbound().name,
        query_str='''FOR o_p_0 IN company'''
                  '''  FOR p_v, p_e IN 1..1 OUTBOUND o_p_0._id'''
                  '''    RETURN p_e.name''',
        bind_vars={},
        returns='p_e.name',
        result=VALUE_RESULT,
    )


def test_edge_target_attribute():
    compare_query(
        query=Company.match().outbound().to().name,
        query_str='''FOR o_p_0 IN company'''
                  '''  FOR p_v, p_e IN 1..1 OUTBOUND o_p_0._id'''
                  '''    RETURN p_v.name''',
        bind_vars={},
        returns='p_v.name',
        result=VALUE_RESULT,
    )


def test_edge_target_edge_attribute():
    compare_query(
        query=Company.match().outbound().to().outbound().name,
        query_str='''FOR o_p_0_0 IN company'''
                  '''  FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p_0_0._id'''
                  '''    FOR p_v, p_e IN 1..1 OUTBOUND p_0_v._id'''
                  '''       RETURN p_e.name''',
        bind_vars={},
        returns='p_e.name',
        result=VALUE_RESULT,
    )


def test_edge_target_edge_target_attribute():
    compare_query(
        query=Company.match().outbound().to().outbound().name,
        query_str='''FOR o_p_0_0 IN company'''
                  '''  FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p_0_0._id'''
                  '''    FOR p_v, p_e IN 1..1 OUTBOUND p_0_v._id'''
                  '''       RETURN p_e.name''',
        bind_vars={},
        returns='p_e.name',
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


def test_array_at_index_attribute():
    compare_query(
        query=Company.match().as_var('a').array(Company.match(industry=var('a').industry))[0].industry,
        query_str='''FOR o_p_0 IN company'''
                  '''  LET a = o_p_0'''
                  '''  LET oqr_p = o_p_0'''
                  '''  LET array_p = ('''
                  '''    FOR o_p_1 IN company'''
                  '''     FILTER o_p_1.industry == a.industry'''
                  '''     RETURN o_p_1'''
                  '''  )'''
                  '''  RETURN array_p[0].industry''',
        bind_vars={},
        returns='array_p[0].industry',
        result=VALUE_RESULT
    )


