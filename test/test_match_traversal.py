from query import *
from test.test_classes import Company
from test.test_utility import compare_query


def test_has_edges():
    compare_query(
        query=Company.match(out()),
        query_str='FOR o_p IN company'
                  '  LET p_1_sub = ('
                  '     FOR p_1_v, p_1_e IN 1..1 OUTBOUND o_p._id'
                  '         RETURN 1'
                  '  )'
                  '  FILTER LENGTH(p_1_sub) > 0'
                  '  RETURN o_p',
        bind_vars={},
        returns='o_p',
        result=Company
    )


def test_has_edges_that_match():
    compare_query(
        query=Company.match(out().match(gt('numerical', 42), field='value')),
        query_str='FOR o_p IN company'
                  '  LET p_1_sub = ('
                  '     FOR p_1_v, p_1_e IN 1..1 OUTBOUND o_p._id'
                  '       FILTER p_1_e.numerical > @p_1_0'
                  '       FILTER p_1_e.field == @p_1_1'
                  '       RETURN 1'
                  '  )'
                  '  FILTER LENGTH(p_1_sub) > 0'
                  '  RETURN o_p',
        bind_vars={'p_1_0': 42, 'p_1_1': 'value'},
        returns='o_p',
        result=Company
    )


def test_has_edge_vertices():
    compare_query(
        query=Company.match(out().to()),
        query_str='FOR o_p IN company'
                  '  LET p_1_sub = ('
                  '     FOR p_1_v, p_1_e IN 1..1 OUTBOUND o_p._id'
                  '         RETURN 1'
                  '  )'
                  '  FILTER LENGTH(p_1_sub) > 0'
                  '  RETURN o_p',
        bind_vars={},
        returns='o_p',
        result=Company
    )


def test_has_edge_vertices_that_match():
    compare_query(
        query=Company.match(out().to().match(gt('numerical', 42), field='value')),
        query_str='FOR o_p IN company'
                  '  LET p_1_sub = ('
                  '     FOR p_1_v, p_1_e IN 1..1 OUTBOUND o_p._id'
                  '       FILTER p_1_v.numerical > @p_1_0'
                  '       FILTER p_1_v.field == @p_1_1'
                  '       RETURN 1'
                  '  )'
                  '  FILTER LENGTH(p_1_sub) > 0'
                  '  RETURN o_p',
        bind_vars={'p_1_0': 42, 'p_1_1': 'value'},
        returns='o_p',
        result=Company
    )


def test_mahas_edge_and_edge_vertices_that_match():
    compare_query(
        query=Company.match(eq('name', 'name'), out().match(like('textual', '%avocado%'), field='val').to().match(gt('numerical', 42), field='value'), this_field='this_field'),
        query_str='FOR o_p IN company'
                  '  FILTER o_p.name == @p_1'
                  '  LET p_2_sub = ('
                  '     FOR p_2_v, p_2_e IN 1..1 OUTBOUND o_p._id'
                  '       FILTER p_2_v.numerical > @p_2_0'
                  '       FILTER p_2_v.field == @p_2_1'
                  '       FILTER p_2_e.textual LIKE @p_2_2'
                  '       FILTER p_2_e.field == @p_2_3'
                  '       RETURN 1'
                  '  )'
                  '  FILTER LENGTH(p_2_sub) > 0'
                  '  FILTER o_p.this_field == @p_6'
                  '  RETURN o_p',
        bind_vars={'p_1': 'name', 'p_2_0': 42, 'p_2_1': 'value', 'p_2_2': '%avocado%', 'p_2_3': 'val', 'p_6': 'this_field'},
        returns='o_p',
        result=Company
    )


