from query import *
from test.test_classes import Company, SubsidiaryOf, Country, LocatedIn
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


def test_has_edge_and_edge_vertices_that_match():
    compare_query(
        query=Company.match(eq('name', 'name'),
                            out().match(like('textual', '%avocado%'), field='val').to().match(gt('numerical', 42),
                                                                                              field='value'),
                            this_field='this_field'),
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
        bind_vars={'p_1': 'name', 'p_2_0': 42, 'p_2_1': 'value', 'p_2_2': '%avocado%', 'p_2_3': 'val',
                   'p_6': 'this_field'},
        returns='o_p',
        result=Company
    )


def test_has_edges_that_match_to():
    # equivalent to Company.match(out()) and Company.match(out().to())

    compare_query(
        query=Company.match(out().match(to())),
        query_str='FOR o_p IN company'
                  '  LET p_1_sub = ('
                  '     FOR p_1_v, p_1_e IN 1..1 OUTBOUND o_p._id'
                  '       LET p_1_0_sub = ('
                  '         RETURN 1'
                  '       )'
                  '       FILTER LENGTH(p_1_0_sub) > 0 '
                  '       RETURN 1'
                  '  )'
                  '  FILTER LENGTH(p_1_sub) > 0'
                  '  RETURN o_p',
        bind_vars={},
        returns='o_p',
        result=Company
    )


def test_has_edges_that_match_to_collection():
    # equivalent to Company.match(out().to(Company))

    compare_query(
        query=Company.match(out().match(to(Company))),
        query_str='''FOR o_p IN company'''
                  '''  LET p_1_sub = ('''
                  '''     FOR p_1_v, p_1_e IN 1..1 OUTBOUND o_p._id'''
                  '''       LET p_1_0_sub = ('''
                  '''         FILTER IS_SAME_COLLECTION('company', p_1_v) '''
                  '''         RETURN 1'''
                  '''       )'''
                  '''       FILTER LENGTH(p_1_0_sub) > 0'''
                  '''       RETURN 1'''
                  '''  )'''
                  '''  FILTER LENGTH(p_1_sub) > 0'''
                  '''  RETURN o_p''',
        bind_vars={},
        returns='o_p',
        result=Company
    )


def test_has_edges_of_collection_that_match_to_collection():
    compare_query(
        query=Company.match(out(LocatedIn, SubsidiaryOf).match(field='value').to(Company, Country).match(field='value'),
                            field='value'),
        query_str='''FOR o_p IN company'''
                  '''  LET p_1_sub = ('''
                  '''     FOR p_1_v, p_1_e IN 1..1 OUTBOUND o_p._id located_at, subsidiary_of'''
                  '''       FILTER IS_SAME_COLLECTION('company',p_1_v) OR IS_SAME_COLLECTION('country',p_1_v)'''
                  '''       FILTER p_1_v.field == @p_1_0'''
                  '''       FILTER p_1_e.field == @p_1_1'''
                  '''       RETURN 1'''
                  '''  )'''
                  '''  FILTER LENGTH(p_1_sub) > 0'''
                  '''  FILTER o_p.field == @p_3'''
                  '''  RETURN o_p''',
        bind_vars={'p_1_0': 'value', 'p_1_1': 'value', 'p_3': 'value'},
        returns='o_p',
        result=Company
    )


def test_has_edges_of_collection_that_match_to_collection_inner():
    compare_query(
        query=Company.match(
            out(LocatedIn, SubsidiaryOf).match(to(Company, Country).match(field='value'), field='value'),
            field='value'),
        query_str='''FOR o_p IN company'''
                  '''  LET p_1_sub = ('''
                  '''     FOR p_1_v, p_1_e IN 1..1 OUTBOUND o_p._id located_at, subsidiary_of'''
                  '''       LET p_1_0_sub = ('''
                  '''         FILTER IS_SAME_COLLECTION('company',p_1_v) OR IS_SAME_COLLECTION('country',p_1_v)'''
                  '''         FILTER p_1_v.field == @p_1_0_0'''
                  '''         RETURN 1'''
                  '''       )'''
                  '''       FILTER LENGTH(p_1_0_sub) > 0'''
                  '''       FILTER p_1_e.field == @p_1_1'''
                  '''       RETURN 1'''
                  '''  )'''
                  '''  FILTER LENGTH(p_1_sub) > 0'''
                  '''  FILTER o_p.field == @p_3'''
                  '''  RETURN o_p''',
        bind_vars={'p_1_1': 'value', 'p_1_0_0': 'value', 'p_3': 'value'},
        returns='o_p',
        result=Company
    )


def test_has_edges_of_collection_that_match_to_collection_multiple():
    compare_query(
        query=Company.match(out(LocatedIn, SubsidiaryOf).match(field='value').to(Company, Country).match(field='value'),
                            field='value').inbound(LocatedIn, min_depth=2, max_depth=5).match(to(Company),
                                                                                              field='value'),
        query_str='''FOR o_p_0 IN company'''
                  '''  LET p_0_1_sub = ('''
                  '''     FOR p_0_1_v, p_0_1_e IN 1..1 OUTBOUND o_p_0._id located_at, subsidiary_of'''
                  '''       FILTER IS_SAME_COLLECTION('company',p_0_1_v) OR IS_SAME_COLLECTION('country',p_0_1_v)'''
                  '''       FILTER p_0_1_v.field == @p_0_1_0'''
                  '''       FILTER p_0_1_e.field == @p_0_1_1'''
                  '''       RETURN 1'''
                  '''  )'''
                  '''  FILTER LENGTH(p_0_1_sub) > 0'''
                  '''  FILTER o_p_0.field == @p_0_3'''
                  '''  FOR p_v, p_e IN 2..5 INBOUND o_p_0._id located_at'''
                  '''     LET p_0_sub = ('''
                  '''       FILTER IS_SAME_COLLECTION('company', p_v)'''
                  '''         RETURN 1'''
                  '''     )'''
                  '''     FILTER LENGTH(p_0_sub) > 0'''
                  '''     FILTER p_e.field == @p_0'''
                  '''  RETURN p_e''',
        bind_vars={'p_0': 'value', 'p_0_1_0': 'value', 'p_0_1_1': 'value', 'p_0_3': 'value'},
        returns='p_e',
        result=AnyResult([LocatedIn])
    )


def test_has_edges_of_collection_that_match_to_collection_multiple_inner():
    compare_query(
        query=Company.match(
                out(LocatedIn, SubsidiaryOf).match(field1='value').to(Company, Country).match(field2='value').inbound(
                LocatedIn, min_depth=2, max_depth=5).match(to(Company), field3='value'), field4='value'),
        query_str='''FOR o_p IN company'''
                  '''  LET p_1_sub = ('''
                  '''    FOR p_1_0_v, p_1_0_e IN 1..1 OUTBOUND p_1_0_v._id located_at,subsidiary_of'''
                  '''      FILTER IS_SAME_COLLECTION('company', p_1_0_v) OR IS_SAME_COLLECTION('country', p_1_0_v)'''
                  '''      FILTER p_1_0_v.field2 == @p_1_0_1'''
                  '''      FILTER p_1_0_e.field1 == @p_1_0_2'''
                  '''      FOR p_1_v, p_1_e IN 2..5 INBOUND p_1_0_v._id located_at'''
                  '''        LET p_1_0_sub = ('''
                  '''          FILTER IS_SAME_COLLECTION('company', p_1_v)'''
                  '''            RETURN 1'''
                  '''        )'''
                  '''        FILTER LENGTH(p_1_0_sub) > 0'''
                  '''    FILTER p_1_e.field3 == @p_1_0'''
                  '''    RETURN 1'''
                  '''  )'''
                  '''  FILTER LENGTH(p_1_sub) > 0'''
                  '''  FILTER o_p.field4 == @p_4'''
                  '''  RETURN o_p''',
        bind_vars={'p_4': 'value', 'p_1_0_1': 'value', 'p_1_0_2': 'value', 'p_1_0': 'value'},
        returns='o_p',
        result=Company
    )
