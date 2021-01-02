from _result import DOCUMENT_RESULT
from cursor._document_cursor import outbound, to, inbound
from cursor.filters._attribute_filters import gt, like, eq
from test.test_cursor.test_classes import Company, SubsidiaryOf, Country, LocatedIn
from test.test_cursor.test_utility import compare_query, TEST_DB


def test_has_edges():
    compare_query(
        cursor=TEST_DB.get(Company).match(outbound().count() > 0),
        query_str='FOR o_p IN company'
                  '  LET p_0_sub = LENGTH(('
                  '     FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p._id'
                  '         RETURN 1'
                  '  ))'
                  '  FILTER p_0_sub > @p_0_compare'
                  '  RETURN o_p',
        bind_vars={'p_0_compare': 0},
        returns='o_p',
        result=Company
    )


def test_has_edges_that_match():
    compare_query(
        cursor=TEST_DB.get(Company).match(outbound().match(gt('numerical', 42), field='value').count() > 0),
        query_str='FOR o_p IN company'
                  '  LET p_0_sub = LENGTH(('
                  '     FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p._id'
                  '       FILTER p_0_e.numerical > @p_0_0'
                  '       FILTER p_0_e.field == @p_0_1'
                  '       RETURN 1'
                  '  ))'
                  '  FILTER p_0_sub > @p_0_compare'
                  '  RETURN o_p',
        bind_vars={'p_0_compare': 0, 'p_0_0': 42, 'p_0_1': 'value'},
        returns='o_p',
        result=Company
    )


def test_has_edge_vertices():
    compare_query(
        cursor=TEST_DB.get(Company).match(outbound().to().count() > 0),
        query_str='FOR o_p IN company'
                  '  LET p_0_sub = LENGTH(('
                  '     FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p._id'
                  '         RETURN 1'
                  '  ))'
                  '  FILTER p_0_sub > @p_0_compare'
                  '  RETURN o_p',
        bind_vars={'p_0_compare': 0},
        returns='o_p',
        result=Company
    )


def test_has_edge_vertices_that_match():
    compare_query(
        cursor=TEST_DB.get(Company).match(outbound().to().match(gt('numerical', 42), field='value').count() == 0),
        query_str='FOR o_p IN company'
                  '  LET p_0_sub = LENGTH(('
                  '     FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p._id'
                  '       FILTER p_0_v.numerical > @p_0_1'
                  '       FILTER p_0_v.field == @p_0_2'
                  '       RETURN 1'
                  '  ))'
                  '  FILTER p_0_sub == @p_0_compare'
                  '  RETURN o_p',
        bind_vars={'p_0_1': 42, 'p_0_2': 'value', 'p_0_compare': 0},
        returns='o_p',
        result=Company
    )


def test_has_edge_and_edge_vertices_that_match():
    compare_query(
        cursor=TEST_DB.get(Company).match(eq('name', 'name'),
                            outbound().match(like('textual', '%avocado%'), field='val').to().match(gt('numerical', 42),
                                                                                              field='value').count() < 0,
                            this_field='this_field'),
        query_str='FOR o_p IN company'
                  '  FILTER o_p.name == @p_0'
                  '  LET p_1_sub = LENGTH(('
                  '     FOR p_1_v, p_1_e IN 1..1 OUTBOUND o_p._id'
                  '       FILTER p_1_v.numerical > @p_1_1'
                  '       FILTER p_1_v.field == @p_1_2'
                  '       FILTER p_1_e.textual LIKE @p_1_3'
                  '       FILTER p_1_e.field == @p_1_4'
                  '       RETURN 1'
                  '  ))'
                  '  FILTER p_1_sub < @p_1_compare'
                  '  FILTER o_p.this_field == @p_6'
                  '  RETURN o_p',
        bind_vars={'p_0': 'name', 'p_1_1': 42, 'p_1_2': 'value', 'p_1_3': '%avocado%', 'p_1_4': 'val',
                   'p_6': 'this_field', 'p_1_compare': 0},
        returns='o_p',
        result=Company
    )


def test_has_edges_that_match_to():
    # equivalent to Company.match(out()) and Company.match(out().to())

    compare_query(
        cursor=TEST_DB.get(Company).match(outbound().match(to().count() == 1).count() > inbound().count()),
        query_str='FOR o_p IN company'
                  '  LET p_0_sub = LENGTH(('
                  '     FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p._id'
                  '       LET p_0_0_sub = LENGTH(('
                  '         RETURN 1'
                  '       ))'
                  '       FILTER p_0_0_sub == @p_0_0_compare'
                  '       RETURN 1'
                  '  ))'
                  '  LET p_0_csub = LENGTH(('
                  '     FOR p_0_v, p_0_e IN 1..1 INBOUND o_p._id'
                  '       RETURN 1'
                  '  ))'
                  '  FILTER p_0_sub > p_0_csub'
                  '  RETURN o_p',
        bind_vars={'p_0_0_compare': 1},
        returns='o_p',
        result=Company
    )


def test_has_edges_that_match_to_collection():

    compare_query(
        cursor=TEST_DB.get(Company).match(outbound().match(to(Company).count() > 0).count() != 42),
        query_str='''FOR o_p IN company'''
                  '''  LET p_0_sub = LENGTH(('''
                  '''     FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p._id'''
                  '''       LET p_0_0_sub = LENGTH(('''
                  '''         FILTER IS_SAME_COLLECTION('company', p_0_v) '''
                  '''         RETURN 1'''
                  '''       ))'''
                  '''       FILTER p_0_0_sub > @ p_0_0_compare'''
                  '''       RETURN 1'''
                  '''  ))'''
                  '''  FILTER p_0_sub != @p_0_compare'''
                  '''  RETURN o_p''',
        bind_vars={'p_0_compare': 42, 'p_0_0_compare': 0},
        returns='o_p',
        result=Company
    )


def test_has_edges_of_collection_that_match_to_collection():
    compare_query(
        cursor=TEST_DB.get(Company).match(outbound(LocatedIn, SubsidiaryOf).match(field='value').to(Company, Country).match(field='value').count() > 0,
                                          field='value'),
        query_str='''FOR o_p IN company'''
                  '''  LET p_0_sub = LENGTH(('''
                  '''     FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p._id located_in, subsidiary_of'''
                  '''       FILTER IS_SAME_COLLECTION('company',p_0_v) OR IS_SAME_COLLECTION('country',p_0_v)'''
                  '''       FILTER p_0_v.field == @p_0_1'''
                  '''       FILTER p_0_e.field == @p_0_2'''
                  '''       RETURN 1'''
                  '''  ))'''
                  '''  FILTER p_0_sub > @p_0_compare'''
                  '''  FILTER o_p.field == @p_3'''
                  '''  RETURN o_p''',
        bind_vars={'p_0_1': 'value', 'p_0_2': 'value', 'p_3': 'value', 'p_0_compare': 0},
        returns='o_p',
        result=Company
    )


def test_has_edges_of_collection_that_match_to_collection_inner():
    compare_query(
        cursor=TEST_DB.get(Company).match(
            outbound(LocatedIn, SubsidiaryOf).match(to(Company, Country).match(field='value').count() != 5, field='value').count() == 5,
            field='value'),
        query_str='''FOR o_p IN company'''
                  '''  LET p_0_sub = LENGTH(('''
                  '''     FOR p_0_v, p_0_e IN 1..1 OUTBOUND o_p._id located_in, subsidiary_of'''
                  '''       LET p_0_0_sub = LENGTH(('''
                  '''         FILTER IS_SAME_COLLECTION('company',p_0_v) OR IS_SAME_COLLECTION('country',p_0_v)'''
                  '''         FILTER p_0_v.field == @p_0_0_1'''
                  '''         RETURN 1'''
                  '''       ))'''
                  '''       FILTER p_0_0_sub != @p_0_0_compare'''
                  '''       FILTER p_0_e.field == @p_0_2'''
                  '''       RETURN 1'''
                  '''  ))'''
                  '''  FILTER p_0_sub == @p_0_compare'''
                  '''  FILTER o_p.field == @p_4'''
                  '''  RETURN o_p''',
        bind_vars={'p_0_2': 'value', 'p_0_0_1': 'value', 'p_4': 'value', 'p_0_compare': 5, 'p_0_0_compare': 5},
        returns='o_p',
        result=Company
    )


def test_has_edges_of_collection_that_match_to_collection_multiple():
    compare_query(
        cursor=TEST_DB.get(Company).match(outbound(LocatedIn, SubsidiaryOf).match(field='value').to(Company, Country).match(field='value').count() > 0, field='value').inbound(LocatedIn, min_depth=2, max_depth=5).match(to(Company).count() > 0,
                                                                                              field='value'),
        query_str='''FOR o_p_0 IN company'''
                  '''  LET p_0_0_sub = LENGTH(('''
                  '''     FOR p_0_0_v, p_0_0_e IN 1..1 OUTBOUND o_p_0._id located_in, subsidiary_of'''
                  '''       FILTER IS_SAME_COLLECTION('company',p_0_0_v) OR IS_SAME_COLLECTION('country',p_0_0_v)'''
                  '''       FILTER p_0_0_v.field == @p_0_0_1'''
                  '''       FILTER p_0_0_e.field == @p_0_0_2'''
                  '''       RETURN 1'''
                  '''  ))'''
                  '''  FILTER p_0_0_sub > @p_0_0_compare'''
                  '''  FILTER o_p_0.field == @p_0_3'''
                  '''  FOR p_v, p_e IN 2..5 INBOUND o_p_0._id located_in'''
                  '''     LET p_0_sub = LENGTH(('''
                  '''       FILTER IS_SAME_COLLECTION('company', p_v)'''
                  '''         RETURN 1'''
                  '''     ))'''
                  '''     FILTER p_0_sub > @p_0_compare'''
                  '''     FILTER p_e.field == @p_1'''
                  '''  RETURN p_e''',
        bind_vars={'p_1': 'value', 'p_0_0_1': 'value', 'p_0_0_2': 'value', 'p_0_3': 'value', 'p_0_0_compare': 0, 'p_0_compare': 0},
        returns='p_e',
        result=DOCUMENT_RESULT
   )


def test_has_edges_of_collection_that_match_to_collection_multiple_inner():
    compare_query(
        cursor=TEST_DB.get(Company).match(
                outbound(LocatedIn, SubsidiaryOf).match(field1='value').to(Company, Country).match(field2='value').inbound(
                LocatedIn, min_depth=2, max_depth=5).match(to(Company).count() > 0, field3='value').count() > 0, field4='value'),
        query_str='''FOR o_p IN company'''
                  '''  LET p_0_sub = LENGTH(('''
                  '''    FOR p_0_0_v, p_0_0_e IN 1..1 OUTBOUND o_p._id located_in,subsidiary_of'''
                  '''      FILTER IS_SAME_COLLECTION('company', p_0_0_v) OR IS_SAME_COLLECTION('country', p_0_0_v)'''
                  '''      FILTER p_0_0_v.field2 == @p_0_0_1'''
                  '''      FILTER p_0_0_e.field1 == @p_0_0_2'''
                  '''      FOR p_0_v, p_0_e IN 2..5 INBOUND p_0_0_v._id located_in'''
                  '''        LET p_0_0_sub = LENGTH(('''
                  '''          FILTER IS_SAME_COLLECTION('company', p_0_v)'''
                  '''            RETURN 1'''
                  '''        ))'''
                  '''        FILTER p_0_0_sub > @p_0_0_compare'''
                  '''    FILTER p_0_e.field3 == @p_0_1'''
                  '''    RETURN 1'''
                  '''  ))'''
                  '''  FILTER p_0_sub > @p_0_compare'''
                  '''  FILTER o_p.field4 == @p_5'''
                  '''  RETURN o_p''',
        bind_vars={'p_0_1': 'value', 'p_0_0_1': 'value', 'p_0_0_2': 'value', 'p_0_0_compare': 0, 'p_0_compare': 0, 'p_5': 'value'},
        returns='o_p',
        result=Company
    )

