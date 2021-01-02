
from _result import DictResult, ListResult, AnyResult
from cursor._document_cursor import outbound
from cursor._var import var
from test.test_cursor.test_classes import Company, LocatedIn, Country
from test.test_cursor.test_utility import compare_query, TEST_DB


def test_array_traversal():
    compare_query(
        cursor=TEST_DB.get(Company).match().as_var('a').array(outbound(LocatedIn).to(Country)).as_var('b').select(company=var('a'),
                                                                                               countries=var('b')),
        query_str='''FOR o_p_0_0 IN company'''
                  '''  LET a = o_p_0_0'''
                  '''  LET oqr_p_0 = o_p_0_0'''
                  '''  LET array_p_0 = ('''
                  '''    FOR p_0_1_v, p_0_1_e IN 1..1 OUTBOUND oqr_p_0._id located_in'''
                  '''      FILTER IS_SAME_COLLECTION('country',p_0_1_v)'''
                  '''      RETURN p_0_1_v'''
                  '''  )'''
                  '''  LET b = array_p_0'''
                  '''  RETURN {'''
                  '''    @p_2: (a),'''
                  '''    @p_4: (b)'''
                  '''  }''',
        bind_vars={'p_2': 'company', 'p_4': 'countries'},
        returns=None,
        result=DictResult(display_name_to_result={'company': Company, 'countries': ListResult(inner_result=AnyResult(inner_result=[Country]))})
    )


def test_array_match():
    compare_query(
        query=Company.match().as_var('a').array(Company.match(industry=var('a').industry)),
        query_str='''FOR o_p_0 IN company'''
                  '''  LET a = o_p_0'''
                  '''  LET oqr_p = o_p_0'''
                  '''  LET array_p = ('''
                  '''    FOR o_p_1 IN company'''
                  '''     FILTER o_p_1.industry == a.industry'''
                  '''     RETURN o_p_1'''
                  '''  )'''
                  '''  RETURN array_p''',
        bind_vars={},
        returns='array_p',
        result=ListResult(inner_result=Company)
    )


def test_array_at_index():
    compare_query(
        query=Company.match().as_var('a').array(Company.match(industry=var('a').industry))[0],
        query_str='''FOR o_p_0 IN company'''
                  '''  LET a = o_p_0'''
                  '''  LET oqr_p = o_p_0'''
                  '''  LET array_p = ('''
                  '''    FOR o_p_1 IN company'''
                  '''     FILTER o_p_1.industry == a.industry'''
                  '''     RETURN o_p_1'''
                  '''  )'''
                  '''  RETURN array_p[0]''',
        bind_vars={},
        returns='array_p[0]',
        result=Company
    )
