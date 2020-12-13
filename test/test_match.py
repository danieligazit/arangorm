from _query import *
from test.test_classes import Company
from test.test_utility import compare_query


def test_return_all_from_collection():
    compare_query(
        query=Company.match(),
        query_str='FOR o_p IN company RETURN o_p',
        bind_vars={},
        returns='o_p',
        result=Company
    )

def test_one_key_value_filter():
    compare_query(
        query=Company.match(some='value'),
        query_str='FOR o_p IN company'
                  '  FILTER o_p.some == @p_1 RETURN o_p',
        bind_vars={'p_1': 'value'},
        returns='o_p',
        result=Company
    )


def test_several_key_value_filters():
    compare_query(
        query=Company.match(some1='value', some2=42, some3=True, some4=3.1415),
        query_str='FOR o_p IN company'
                  '  FILTER o_p.some1 == @p_1'
                  '  FILTER o_p.some2 == @p_2'
                  '  FILTER o_p.some3 == @p_3'
                  '  FILTER o_p.some4 == @p_4'
                  '  RETURN o_p',
        bind_vars={'p_1': 'value', 'p_2': 42, 'p_3': True, 'p_4': 3.1415},
        returns='o_p',
        result=Company
    )


def test_multiple_matches():
    compare_query(
        query=Company.match(some1='value').match(some2=42, some3=True).match(some4=3.1415),
        query_str='FOR o_p IN company'
                  '  FILTER o_p.some1 == @p_1'
                  '  FILTER o_p.some2 == @p_2'
                  '  FILTER o_p.some3 == @p_3'
                  '  FILTER o_p.some4 == @p_4'
                  '  RETURN o_p',
        bind_vars={'p_1': 'value', 'p_2': 42, 'p_3': True, 'p_4': 3.1415},
        returns='o_p',
        result=Company
    )


def test_gt():
    compare_query(
        query=Company.match(gt('some1', 5)),
        query_str='FOR o_p IN company'
                  '  FILTER o_p.some1 > @p_1'
                  '  RETURN o_p',
        bind_vars={'p_1': 5},
        returns='o_p',
        result=Company
    )


def test_like():
    compare_query(
        query=Company.match(like('some1', 'avocado%')),
        query_str='FOR o_p IN company'
                  '  FILTER o_p.some1 LIKE @p_1'
                  '  RETURN o_p',
        bind_vars={'p_1': 'avocado%'},
        returns='o_p',
        result=Company
    )


def test_multiple_operators():
    compare_query(
        query=Company.match(like('some_string', 'avocado%'), gt('some_number', 42)).match(eq('some_value', 'value')),
        query_str='FOR o_p IN company'
                  '  FILTER o_p.some_string LIKE @p_1'
                  '  FILTER o_p.some_number > @p_2'
                  '  FILTER o_p.some_value == @p_3'
                  '  RETURN o_p',
        bind_vars={'p_1': 'avocado%', 'p_2': 42, 'p_3': 'value'},
        returns='o_p',
        result=Company
    )


def test_multipltes_matches_and_operators():
    compare_query(
        query=Company.match(like('some_string', 'avocado%'), gt('some_number', 42), some_condition=True).match(
            eq('some_object.field', 'value'), another_value=[4, 5]),
        query_str='FOR o_p IN company'
                  '  FILTER o_p.some_string LIKE @p_1'
                  '  FILTER o_p.some_number > @p_2'
                  '  FILTER o_p.some_condition == @p_3'
                  '  FILTER o_p.some_object.field == @p_4'
                  '  FILTER o_p.another_value == @p_5'
                  '  RETURN o_p',
        bind_vars={'p_1': 'avocado%', 'p_2': 42, 'p_3': True, 'p_4': 'value', 'p_5': [4, 5]},
        returns='o_p',
        result=Company
    )
