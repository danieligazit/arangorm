from model import Country
from query import *
from test.test_classes import Company, LOCATED_AT, LocatedIn
from test.test_utility import compare_query


def test_group():
    compare_query(
        query=Company.match().group(),
        query_str='FOR o_p_0 IN company'
                  '  COLLECT field__key=o_p_0._key INTO groups = o_p_0'
                  '  RETURN {'
                  '    @p_2: (groups)'
                  '  }',
        bind_vars={'p_2': 'document'},
        returns=None,
        result=DictResult(display_name_to_result={'document': ListResult(inner_result=DOCUMENT_RESULT)})
    )


def test_group_document_by():
    compare_query(
        query=Company.match().group().by(),
        query_str='FOR o_p_0 IN company'
                  '  COLLECT field__key=o_p_0._key INTO groups = o_p_0'
                  '  RETURN {'
                  '    @p_2: (groups)'
                  '  }',
        bind_vars={'p_2': 'document'},
        returns=None,
        result=DictResult(display_name_to_result={'document': ListResult(inner_result=DOCUMENT_RESULT)})
    )


def test_group_document_by_field():
    compare_query(
        query=Company.match().group().by('industry'),
        query_str='FOR o_p_0 IN company'
                  '  COLLECT field_industry=o_p_0.industry INTO groups = o_p_0'
                  '  RETURN {'
                  '    @p_2: (groups)'
                  '  }',
        bind_vars={'p_2': 'document'},
        returns=None,
        result=DictResult(display_name_to_result={'document': ListResult(inner_result=DOCUMENT_RESULT)})
    )


def test_group_field_by_field():
    compare_query(
        query=Company.match().group('industry').by('industry'),
        query_str='FOR o_p_0 IN company'
                  '  COLLECT field_industry=o_p_0.industry INTO groups = o_p_0'
                  '  RETURN {'
                  '    @p_2: (field_industry)'
                  '  }',
        bind_vars={'p_2': 'industry'},
        returns=None,
        result=DictResult(display_name_to_result={'industry': VALUE_RESULT})
    )


def test_group_field_and_document_by_field():
    compare_query(
        query=Company.match().group('industry', object).by('industry'),
        query_str='FOR o_p_0 IN company'
                  '  COLLECT field_industry=o_p_0.industry INTO groups = o_p_0'
                  '  RETURN {'
                  '    @p_2: (field_industry),'
                  '    @p_4: (groups)'
                  '  }',
        bind_vars={'p_2': 'industry', 'p_4': 'document'},
        returns=None,
        result=DictResult(
            display_name_to_result={'industry': VALUE_RESULT, 'document': ListResult(inner_result=DOCUMENT_RESULT)})
    )


def test_group_field_document_and_count_by_field():
    compare_query(
        query=Company.match().group('industry', doc=object, industry_count=count('industry')).by('industry'),
        query_str='''FOR o_p_0 IN company'''
                  '''  COLLECT field_industry=o_p_0.industry INTO groups = o_p_0'''
                  '''  RETURN {'''
                  '''    @p_2: (field_industry),'''
                  '''    @p_4: (groups),'''
                  '''    @p_6: (COUNT(groups[*].industry))'''
                  '''  }''',
        bind_vars={'p_2': 'industry', 'p_4': 'doc', 'p_6': 'industry_count'},
        returns=None,
        result=DictResult(
            display_name_to_result={'industry': VALUE_RESULT, 'document': ListResult(inner_result=DOCUMENT_RESULT),
                                    'industry_count': VALUE_RESULT})
    )


def test_group_field_document_and_count_by_field():
    compare_query(
        query=Company.match().group('industry', max('employee_number'), companies=object).by('industry'),
        query_str='''FOR o_p_0 IN company'''
                  '''  COLLECT field_industry=o_p_0.industry INTO groups = o_p_0'''
                  '''  RETURN {'''
                  '''    @p_2: (field_industry),'''
                  '''    @p_4: (MAX(groups[*].employee_number)),'''
                  '''    @p_6: (groups)'''
                  '''  }''',
        bind_vars={'p_2': 'industry', 'p_4': 'max_employee_number', 'p_6': 'companies'},
        returns=None,
        result=DictResult(
            display_name_to_result={'industry': VALUE_RESULT, 'max_employee_number': VALUE_RESULT,
                                    'companies': ListResult(DOCUMENT_RESULT)})
    )


def test_group_traversal():
    compare_query(
        query=Company.match().group('industry', edges=out(LocatedIn), edge_targets=out(LocatedIn).to(Country)).by(
            'industry'),
        query_str='''FOR o_p_0 IN company'''
                  '''  COLLECT field_industry=o_p_0.industry INTO groups = o_p_0'''
                  '''  RETURN {'''
                  '''    @p_2: (field_industry),'''
                  '''    @p_4: ('''
                  '''      FOR p_3_doc in groups[*]'''
                  '''        FOR p_3_v, p_3_e IN 1..1 OUTBOUND p_3_doc._id located_at'''
                  '''          RETURN p_3_e'''
                  '''    ),'''
                  '''    @p_6: ('''
                  '''      FOR p_5_doc in groups[*]'''
                  '''        FOR p_5_v, p_5_e IN 1..1 OUTBOUND p_5_doc._id located_at'''
                  '''          FILTER IS_SAME_COLLECTION('country', p_5_v)'''
                  '''          RETURN p_5_v'''
                  '''    )'''
                  '''  }''',
        bind_vars={'p_2': 'industry', 'p_4': 'edges', 'p_6': 'edge_targets'},
        returns=None,
        result=DictResult(
            display_name_to_result={'industry': VALUE_RESULT, 'edges': ListResult(AnyResult([LocatedIn])),
                                    'edge_targets': ListResult(AnyResult([Country]))})
    )


def test_group_edge_traversal():
    compare_query(
        query=Company.match().out(LocatedIn).group('industry', edges=object, edge_targets=to(Country)).by('industry'),
        query_str='''FOR o_p_0 IN company'''
                  '''  COLLECT field_industry=o_p_0.industry INTO groups = o_p_0'''
                  '''  RETURN {'''
                  '''    @p_2: (field_industry),'''
                  '''    @p_4: ('''
                  '''      FOR p_3_doc in groups[*]'''
                  '''        FOR p_3_v, p_3_e IN 1..1 OUTBOUND p_3_doc._id located_at'''
                  '''          RETURN p_3_e'''
                  '''    ),'''
                  '''    @p_6: ('''
                  '''      FOR p_5_doc in groups[*]'''
                  '''        FOR p_5_v, p_5_e IN 1..1 OUTBOUND p_5_doc._id located_at'''
                  '''          FILTER IS_SAME_COLLECTION('country', p_5_v)'''
                  '''          RETURN p_5_v'''
                  '''    )'''
                  '''  }''',
        bind_vars={'p_2': 'industry', 'p_4': 'edges', 'p_6': 'edge_targets'},
        returns=None,
        result=DictResult(
            display_name_to_result={'industry': VALUE_RESULT, 'edges': ListResult(AnyResult([LocatedIn])),
                                    'edge_targets': ListResult(AnyResult([Country]))})
    )


test_group_edge_traversal()