from _result import DOCUMENT_RESULT
from cursor._document_cursor import outbound, to
from cursor.filters._attribute_filters import gt, like, eq
from test.test_cursor.test_classes import Company, SubsidiaryOf, Country, LocatedIn
from test.test_cursor.test_utility import compare_query, TEST_DB


def test_has_edges():
    compare_query(
        cursor=TEST_DB.get(Company).match(outbound()),
        query_str='FOR o_p IN company'
                  '  LET p_0_sub = ('
                  '     FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p._id'
                  '         RETURN 1'
                  '  )'
                  '  FILTER LENGTH(p_0_sub) > 0'
                  '  RETURN o_p',
        bind_vars={},
        returns='o_p',
        result=Company
    )


def test_has_edges_that_match():
    compare_query(
        cursor=TEST_DB.get(Company).match(outbound().match(gt('numerical', 42), field='value')),
        query_str='FOR o_p IN company'
                  '  LET p_0_sub = ('
                  '     FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p._id'
                  '       FILTER p_0_e.numerical > @p_0_0'
                  '       FILTER p_0_e.field == @p_0_1'
                  '       RETURN 1'
                  '  )'
                  '  FILTER LENGTH(p_0_sub) > 0'
                  '  RETURN o_p',
        bind_vars={'p_0_0': 42, 'p_0_1': 'value'},
        returns='o_p',
        result=Company
    )


def test_has_edge_vertices():
    compare_query(
        cursor=TEST_DB.get(Company).match(outbound().to()),
        query_str='FOR o_p IN company'
                  '  LET p_0_sub = ('
                  '     FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p._id'
                  '         RETURN 1'
                  '  )'
                  '  FILTER LENGTH(p_0_sub) > 0'
                  '  RETURN o_p',
        bind_vars={},
        returns='o_p',
        result=Company
    )


def test_has_edge_vertices_that_match():
    compare_query(
        cursor=TEST_DB.get(Company).match(outbound().to().match(gt('numerical', 42), field='value')),
        query_str='FOR o_p IN company'
                  '  LET p_0_sub = ('
                  '     FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p._id'
                  '       FILTER p_0_v.numerical > @p_0_0'
                  '       FILTER p_0_v.field == @p_0_1'
                  '       RETURN 1'
                  '  )'
                  '  FILTER LENGTH(p_0_sub) > 0'
                  '  RETURN o_p',
        bind_vars={'p_0_0': 42, 'p_0_1': 'value'},
        returns='o_p',
        result=Company
    )


def test_has_edge_and_edge_vertices_that_match():
    compare_query(
        cursor=TEST_DB.get(Company).match(eq('name', 'name'),
                            outbound().match(like('textual', '%avocado%'), field='val').to().match(gt('numerical', 42),
                                                                                              field='value'),
                            this_field='this_field'),
        query_str='FOR o_p IN company'
                  '  FILTER o_p.name == @p_0'
                  '  LET p_1_sub = ('
                  '     FOR p_1_v, p_1_e IN 1..1 OUTBOUND o_p._id'
                  '       FILTER p_1_v.numerical > @p_1_0'
                  '       FILTER p_1_v.field == @p_1_1'
                  '       FILTER p_1_e.textual LIKE @p_1_2'
                  '       FILTER p_1_e.field == @p_1_3'
                  '       RETURN 1'
                  '  )'
                  '  FILTER LENGTH(p_1_sub) > 0'
                  '  FILTER o_p.this_field == @p_5'
                  '  RETURN o_p',
        bind_vars={'p_0': 'name', 'p_1_0': 42, 'p_1_1': 'value', 'p_1_2': '%avocado%', 'p_1_3': 'val',
                   'p_5': 'this_field'},
        returns='o_p',
        result=Company
    )


def test_has_edges_that_match_to():
    # equivalent to Company.match(out()) and Company.match(out().to())

    compare_query(
        cursor=TEST_DB.get(Company).match(outbound().match(to())),
        query_str='FOR o_p IN company'
                  '  LET p_0_sub = ('
                  '     FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p._id'
                  '       LET p_0_0_sub = ('
                  '         RETURN 1'
                  '       )'
                  '       FILTER LENGTH(p_0_0_sub) > 0 '
                  '       RETURN 1'
                  '  )'
                  '  FILTER LENGTH(p_0_sub) > 0'
                  '  RETURN o_p',
        bind_vars={},
        returns='o_p',
        result=Company
    )


def test_has_edges_that_match_to_collection():
    # equivalent to Company.match(out().to(Company))

    compare_query(
        cursor=TEST_DB.get(Company).match(outbound().match(to(Company))),
        query_str='''FOR o_p IN company'''
                  '''  LET p_0_sub = ('''
                  '''     FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p._id'''
                  '''       LET p_0_0_sub = ('''
                  '''         FILTER IS_SAME_COLLECTION('company', p_0_v) '''
                  '''         RETURN 1'''
                  '''       )'''
                  '''       FILTER LENGTH(p_0_0_sub) > 0'''
                  '''       RETURN 1'''
                  '''  )'''
                  '''  FILTER LENGTH(p_0_sub) > 0'''
                  '''  RETURN o_p''',
        bind_vars={},
        returns='o_p',
        result=Company
    )


def test_has_edges_of_collection_that_match_to_collection():
    compare_query(
        cursor=TEST_DB.get(Company).match(outbound(LocatedIn, SubsidiaryOf).match(field='value').to(Company, Country).match(field='value'),
                            field='value'),
        query_str='''FOR o_p IN company'''
                  '''  LET p_0_sub = ('''
                  '''     FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p._id located_in, subsidiary_of'''
                  '''       FILTER IS_SAME_COLLECTION('company',p_0_v) OR IS_SAME_COLLECTION('country',p_0_v)'''
                  '''       FILTER p_0_v.field == @p_0_0'''
                  '''       FILTER p_0_e.field == @p_0_1'''
                  '''       RETURN 1'''
                  '''  )'''
                  '''  FILTER LENGTH(p_0_sub) > 0'''
                  '''  FILTER o_p.field == @p_2'''
                  '''  RETURN o_p''',
        bind_vars={'p_0_0': 'value', 'p_0_1': 'value', 'p_2': 'value'},
        returns='o_p',
        result=Company
    )


def test_has_edges_of_collection_that_match_to_collection_inner():
    compare_query(
        cursor=TEST_DB.get(Company).match(
            outbound(LocatedIn, SubsidiaryOf).match(to(Company, Country).match(field='value'), field='value'),
            field='value'),
        query_str='''FOR o_p IN company'''
                  '''  LET p_0_sub = ('''
                  '''     FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p._id located_in, subsidiary_of'''
                  '''       LET p_0_0_sub = ('''
                  '''         FILTER IS_SAME_COLLECTION('company',p_0_v) OR IS_SAME_COLLECTION('country',p_0_v)'''
                  '''         FILTER p_0_v.field == @p_0_0_0'''
                  '''         RETURN 1'''
                  '''       )'''
                  '''       FILTER LENGTH(p_0_0_sub) > 0'''
                  '''       FILTER p_0_e.field == @p_0_1'''
                  '''       RETURN 1'''
                  '''  )'''
                  '''  FILTER LENGTH(p_0_sub) > 0'''
                  '''  FILTER o_p.field == @p_2'''
                  '''  RETURN o_p''',
        bind_vars={'p_0_1': 'value', 'p_0_0_0': 'value', 'p_2': 'value'},
        returns='o_p',
        result=Company
    )


def test_has_edges_of_collection_that_match_to_collection_multiple():
    compare_query(
        cursor=TEST_DB.get(Company).match(outbound(LocatedIn, SubsidiaryOf).match(field='value').to(Company, Country).match(field='value'),
                            field='value').inbound(LocatedIn, min_depth=2, max_depth=5).match(to(Company),
                                                                                              field='value'),
        query_str='''FOR o_p_0 IN company'''
                  '''  LET p_0_0_sub = ('''
                  '''     FOR p_0_1_v, p_0_1_e IN 1..1 OUTBOUND o_p_0._id located_in, subsidiary_of'''
                  '''       FILTER IS_SAME_COLLECTION('company',p_0_1_v) OR IS_SAME_COLLECTION('country',p_0_1_v)'''
                  '''       FILTER p_0_1_v.field == @p_0_1_0'''
                  '''       FILTER p_0_1_e.field == @p_0_1_1'''
                  '''       RETURN 1'''
                  '''  )'''
                  '''  FILTER LENGTH(p_0_0_sub) > 0'''
                  '''  FILTER o_p_0.field == @p_0_3'''
                  '''  FOR p_v, p_e IN 2..5 INBOUND o_p_0._id located_in'''
                  '''     LET p_0_sub = ('''
                  '''       FILTER IS_SAME_COLLECTION('company', p_v)'''
                  '''         RETURN 1'''
                  '''     )'''
                  '''     FILTER LENGTH(p_0_sub) > 0'''
                  '''     FILTER p_e.field == @p_0'''
                  '''  RETURN p_e''',
        bind_vars={'p_0': 'value', 'p_0_1_0': 'value', 'p_0_1_1': 'value', 'p_0_3': 'value'},
        returns='p_e',
        result=DOCUMENT_RESULT
   )


def test_has_edges_of_collection_that_match_to_collection_multiple_inner():
    compare_query(
        cursor=TEST_DB.get(Company).match(
                outbound(LocatedIn, SubsidiaryOf).match(field1='value').to(Company, Country).match(field2='value').inbound(
                LocatedIn, min_depth=2, max_depth=5).match(to(Company), field3='value'), field4='value'),
        query_str='''FOR o_p IN company'''
                  '''  LET p_0_sub = ('''
                  '''    FOR p_0_1_v, p_0_1_e IN 1..1 OUTBOUND o_p._id located_in,subsidiary_of'''
                  '''      FILTER IS_SAME_COLLECTION('company', p_0_0_v) OR IS_SAME_COLLECTION('country', p_0_0_v)'''
                  '''      FILTER p_0_0_v.field2 == @p_0_0_1'''
                  '''      FILTER p_0_0_e.field1 == @p_0_0_2'''
                  '''      FOR p_0_v, p_0_e IN 2..5 INBOUND p_0_0_v._id located_in'''
                  '''        LET p_0_0_sub = ('''
                  '''          FILTER IS_SAME_COLLECTION('company', p_0_v)'''
                  '''            RETURN 1'''
                  '''        )'''
                  '''        FILTER LENGTH(p_0_0_sub) > 0'''
                  '''    FILTER p_0_e.field3 == @p_0_0'''
                  '''    RETURN 1'''
                  '''  )'''
                  '''  FILTER LENGTH(p_0_sub) > 0'''
                  '''  FILTER o_p.field4 == @p_3'''
                  '''  RETURN o_p''',
        bind_vars={'p_3': 'value', 'p_0_0_1': 'value', 'p_0_0_2': 'value', 'p_0_0': 'value'},
        returns='o_p',
        result=Company
    )

