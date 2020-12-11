from test.test_classes import Country
from query import *
from test.test_classes import Company, LocatedIn
from test.test_utility import compare_query


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


# def test_group_edge_traversal():
#     compare_query(
#         query=Company.match().as_var('a').array(out(LocatedIn)).as_var('b').select(a=var('a'), b=var('b')),
#         query_str='''FOR o_p_0 IN company'''
#                   '''  COLLECT field_industry=o_p_0.industry INTO groups = o_p_0'''
#                   '''  RETURN {'''
#                   '''    @p_2: (field_industry),'''
#                   '''    @p_4: ('''
#                   '''      FOR p_3_doc in groups[*]'''
#                   '''        FOR p_3_v, p_3_e IN 1..1 OUTBOUND p_3_doc._id located_at'''
#                   '''          RETURN p_3_e'''
#                   '''    ),'''
#                   '''    @p_6: ('''
#                   '''      FOR p_5_doc in groups[*]'''
#                   '''        FOR p_5_v, p_5_e IN 1..1 OUTBOUND p_5_doc._id located_at'''
#                   '''          FILTER IS_SAME_COLLECTION('country', p_5_v)'''
#                   '''          RETURN p_5_v'''
#                   '''    )'''
#                   '''  }''',
#         bind_vars={'p_2': 'industry', 'p_4': 'edges', 'p_6': 'edge_targets'},
#         returns=None,
#         result=DictResult(
#             display_name_to_result={'industry': VALUE_RESULT, 'edges': ListResult(AnyResult([LocatedIn])),
#                                     'edge_targets': ListResult(AnyResult([Country]))})
#     )
#
# test_group_edge_traversal()