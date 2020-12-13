from _query import *
from test.test_classes import Company, LocatedIn, Country, SubsidiaryOf
from test.test_utility import compare_query


def test_edge_no_collection():
    compare_query(
        query=Company.match().out(),
        query_str='FOR o_p_0 IN company'
                  '  FOR p_v, p_e IN 1..1 OUTBOUND o_p_0._id'
                  '   RETURN p_e',
        bind_vars={},
        returns='p_e',
        result=DOCUMENT_RESULT
    )


def test_vertex_no_collection():
    compare_query(
        query=Company.match().out().to(),
        query_str='FOR o_p_0 IN company'
                  '  FOR p_v, p_e IN 1..1 OUTBOUND o_p_0._id'
                  '   RETURN p_v',
        bind_vars={},
        returns='p_v',
        result=DOCUMENT_RESULT
    )


def test_edge_outbound():
    compare_query(
        query=Company.match().out(LocatedIn),
        query_str='FOR o_p_0 IN company'
                  '  FOR p_v, p_e IN 1..1 OUTBOUND o_p_0._id located_at'
                  '   RETURN p_e',
        bind_vars={},
        returns='p_e',
        result=AnyResult([LocatedIn])
    )


def test_edge_inbound():
    compare_query(
        query=Company.match().inbound(LocatedIn),
        query_str='FOR o_p_0 IN company'
                  '  FOR p_v, p_e IN 1..1 INBOUND o_p_0._id located_at'
                  '   RETURN p_e',
        bind_vars={},
        returns='p_e',
        result=AnyResult([LocatedIn])
    )


def test_edge_any():
    compare_query(
        query=Company.match().connected_by(LocatedIn),
        query_str='FOR o_p_0 IN company'
                  '  FOR p_v, p_e IN 1..1 ANY o_p_0._id located_at'
                  '   RETURN p_e',
        bind_vars={},
        returns='p_e',
        result=AnyResult([LocatedIn])
    )


def test_edge_outbound_multiple_edges():
    compare_query(
        query=Company.match().out(LocatedIn, SubsidiaryOf),
        query_str='FOR o_p_0 IN company'
                  '  FOR p_v, p_e IN 1..1 OUTBOUND o_p_0._id located_at, subsidiary_of'
                  '   RETURN p_e',
        bind_vars={},
        returns='p_e',
        result=AnyResult([LocatedIn, SubsidiaryOf])
    )


def test_vertex_outbound():
    compare_query(
        query=Company.match().out(LocatedIn).to(Country),
        query_str='''FOR o_p_0 IN company'''
                  '''  FOR p_v, p_e IN 1..1 OUTBOUND o_p_0._id located_at'''
                  '''    FILTER IS_SAME_COLLECTION('country', p_v)'''
                  '''    RETURN p_v''',
        bind_vars={},
        returns='p_v',
        result=AnyResult([Country])
    )


def test_outbound_multiple_targets():
    compare_query(
        query=Company.match().out(LocatedIn).to(Country, Company),
        query_str='''FOR o_p_0 IN company'''
                  '''  FOR p_v, p_e IN 1..1 OUTBOUND o_p_0._id located_at'''
                  '''    FILTER IS_SAME_COLLECTION('country', p_v) OR IS_SAME_COLLECTION('company', p_v)'''
                  '''    RETURN p_v''',
        bind_vars={},
        returns='p_v',
        result=AnyResult([Country, Company])
    )


def test_varying_min_depth():
    compare_query(
        query=Company.match().out(LocatedIn, min_depth=2),
        query_str='''FOR o_p_0 IN company'''
                  '''  FOR p_v, p_e IN 2..2 OUTBOUND o_p_0._id located_at'''
                  '''    RETURN p_e''',
        bind_vars={},
        returns='p_e',
        result=AnyResult([LocatedIn])
    )


def test_varying_max_depth():
    compare_query(
        query=Company.match().out(LocatedIn, max_depth=5),
        query_str='''FOR o_p_0 IN company'''
                  '''  FOR p_v, p_e IN 1..5 OUTBOUND o_p_0._id located_at'''
                  '''    RETURN p_e''',
        bind_vars={},
        returns='p_e',
        result=AnyResult([LocatedIn])
    )


def test_varying_depth_outbound():
    compare_query(
        query=Company.match().out(LocatedIn, min_depth=2, max_depth=5),
        query_str='''FOR o_p_0 IN company'''
                  '''  FOR p_v, p_e IN 2..5 OUTBOUND o_p_0._id located_at'''
                  '''    RETURN p_e''',
        bind_vars={},
        returns='p_e',
        result=AnyResult([LocatedIn])
    )


def test_varying_depth_inbound():
    compare_query(
        query=Company.match().inbound(LocatedIn, min_depth=2, max_depth=5),
        query_str='''FOR o_p_0 IN company'''
                  '''  FOR p_v, p_e IN 2..5 INBOUND o_p_0._id located_at'''
                  '''    RETURN p_e''',
        bind_vars={},
        returns='p_e',
        result=AnyResult([LocatedIn])
    )


def test_varying_depth_outbound_vertex():
    compare_query(
        query=Company.match().out(LocatedIn, min_depth=2, max_depth=5).to(Country),
        query_str='''FOR o_p_0 IN company'''
                  '''  FOR p_v, p_e IN 2..5 OUTBOUND o_p_0._id located_at'''
                  '''    FILTER IS_SAME_COLLECTION('country', p_v)'''
                  '''    RETURN p_v''',
        bind_vars={},
        returns='p_v',
        result=AnyResult([Country])
    )


def test_varying_depth_any_vertex():
    compare_query(
        query=Company.match().connected_by(LocatedIn, max_depth=3),
        query_str='''FOR o_p_0 IN company'''
                  '''  FOR p_v, p_e IN 1..3 ANY o_p_0._id located_at'''
                  '''    RETURN p_e''',
        bind_vars={},
        returns='p_e',
        result=AnyResult([LocatedIn])
    )


def test_multilevel_any():
    compare_query(
        query=Company.match().out(SubsidiaryOf, min_depth=2).to(Company).connected_by(LocatedIn, max_depth=3),
        query_str='''FOR o_p_0_0 IN company'''
                  '''  FOR p_0_v, p_0_e IN 2..2 OUTBOUND o_p_0_0._id subsidiary_of'''
                  '''    FILTER IS_SAME_COLLECTION('company', p_0_v)'''
                  '''    FOR p_v, p_e IN 1..3 ANY p_0_v._id located_at'''
                  '''       RETURN p_e''',
        bind_vars={},
        returns='p_e',
        result=AnyResult([LocatedIn])
    )


def test_multilevel_out():
    compare_query(
        query=Company.match().out(SubsidiaryOf, min_depth=2).to(Company).out(LocatedIn, max_depth=3),
        query_str='''FOR o_p_0_0 IN company'''
                  '''  FOR p_0_v, p_0_e IN 2..2 OUTBOUND o_p_0_0._id subsidiary_of'''
                  '''    FILTER IS_SAME_COLLECTION('company', p_0_v)'''
                  '''    FOR p_v, p_e IN 1..3 OUTBOUND p_0_v._id located_at'''
                  '''       RETURN p_e''',
        bind_vars={},
        returns='p_e',
        result=AnyResult([LocatedIn])
    )


def test_multilevel_in():
    compare_query(
        query=Company.match().out(SubsidiaryOf, min_depth=2).to(Company).inbound(LocatedIn, max_depth=3),
        query_str='''FOR o_p_0_0 IN company'''
                  '''  FOR p_0_v, p_0_e IN 2..2 OUTBOUND o_p_0_0._id subsidiary_of'''
                  '''    FILTER IS_SAME_COLLECTION('company', p_0_v)'''
                  '''    FOR p_v, p_e IN 1..3 INBOUND p_0_v._id located_at'''
                  '''       RETURN p_e''',
        bind_vars={},
        returns='p_e',
        result=AnyResult([LocatedIn])
    )


def test_traversal_and_match_origin_vertices():
    compare_query(
        query=Company.match(gt('another_field', 42), field='value').out(SubsidiaryOf, min_depth=2),
        query_str='''FOR o_p_0 IN company'''
                  '''  FILTER o_p_0.another_field > @p_0_1'''
                  '''  FILTER o_p_0.field == @p_0_2'''
                  '''  FOR p_v, p_e IN 2..2 OUTBOUND o_p_0._id subsidiary_of'''
                  '''       RETURN p_e''',
        bind_vars={'p_0_2': 'value', 'p_0_1': 42},
        returns='p_e',
        result=AnyResult([SubsidiaryOf])
    )


def test_traversal_and_match_edge():
    compare_query(
        query=Company.match().out(SubsidiaryOf, min_depth=2).match(gt('another_field', 42), field='value'),
        query_str='''FOR o_p_0 IN company'''
                  '''  FOR p_v, p_e IN 2..2 OUTBOUND o_p_0._id subsidiary_of'''
                  '''    FILTER p_e.another_field > @p_0'''
                  '''    FILTER p_e.field == @p_1'''
                  '''    RETURN p_e''',
        bind_vars={'p_1': 'value', 'p_0': 42},
        returns='p_e',
        result=AnyResult([SubsidiaryOf])
    )


def test_traversal_and_match_target_vertices():
    compare_query(
        query=Company.match().out(SubsidiaryOf, min_depth=2).to().match(gt('another_field', 42), field='value'),
        query_str='''FOR o_p_0 IN company'''
                  '''  FOR p_v, p_e IN 2..2 OUTBOUND o_p_0._id subsidiary_of'''
                  '''    FILTER p_v.another_field > @p_1'''
                  '''    FILTER p_v.field == @p_2'''
                  '''    RETURN p_v''',
        bind_vars={'p_2': 'value', 'p_1': 42},
        returns='p_v',
        result=DOCUMENT_RESULT
    )


def test_traversal_and_match_all():
    compare_query(
        query=Company.match().out(SubsidiaryOf, LocatedIn, min_depth=2).match(eq('value', 3.1415), some_bool=True).to(
            Country, Company).match(gt('another_field', 42), field='value'),
        query_str='''FOR o_p_0 IN company'''
                  '''  FOR p_v, p_e IN 2..2 OUTBOUND o_p_0._id subsidiary_of, located_at'''
                  '''    FILTER IS_SAME_COLLECTION('country', p_v) OR IS_SAME_COLLECTION('company', p_v)'''
                  '''    FILTER p_v.another_field > @p_1'''
                  '''    FILTER p_v.field == @p_2'''
                  '''    FILTER p_e.value == @p_3'''
                  '''    FILTER p_e.some_bool == @p_4'''
                  '''    RETURN p_v''',
        bind_vars={'p_3': 3.1415, 'p_4': True, 'p_1': 42, 'p_2': 'value'},
        returns='p_v',
        result=AnyResult([Country, Company])
    )


