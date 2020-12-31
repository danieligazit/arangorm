from cursor.filters._attribute_filters import like, gt, eq
from test.test_cursor.test_classes import Company
from test.test_cursor.test_utility import compare_query, TEST_DB


def test_return_all_from_collection():
    compare_query(
        cursor=TEST_DB.get(Company),
        query_str='FOR o_p IN company RETURN o_p',
        bind_vars={},
        returns='o_p',
        result=Company
    )


def test_one_key_value_filter():
    compare_query(
        cursor=TEST_DB.get(Company).match(some='value'),
        query_str='FOR o_p IN company'
                  '  FILTER o_p.some == @p_0 '
                  '  RETURN o_p',
        bind_vars={'p_0': 'value'},
        returns='o_p',
        result=Company
    )


def test_several_key_value_filters():
    compare_query(
        cursor=TEST_DB.get(Company).match(some1='value', some2=42, some3=True, some4=3.1415),
        query_str='FOR o_p IN company'
                  '  FILTER o_p.some1 == @p_0'
                  '  FILTER o_p.some2 == @p_1'
                  '  FILTER o_p.some3 == @p_2'
                  '  FILTER o_p.some4 == @p_3'
                  '  RETURN o_p',
        bind_vars={'p_0': 'value', 'p_1': 42, 'p_2': True, 'p_3': 3.1415},
        returns='o_p',
        result=Company
    )


def test_multiple_matches():
    compare_query(
        cursor=TEST_DB.get(Company).match(some1='value').match(some2=42, some3=True).match(some4=3.1415),
        query_str='FOR o_p IN company'
                  '  FILTER o_p.some1 == @p_0'
                  '  FILTER o_p.some2 == @p_1'
                  '  FILTER o_p.some3 == @p_2'
                  '  FILTER o_p.some4 == @p_3'
                  '  RETURN o_p',
        bind_vars={'p_0': 'value', 'p_1': 42, 'p_2': True, 'p_3': 3.1415},
        returns='o_p',
        result=Company
    )


def test_gt():
    compare_query(
        cursor=TEST_DB.get(Company).match(gt('some1', 5)),
        query_str='FOR o_p IN company'
                  '  FILTER o_p.some1 > @p_0'
                  '  RETURN o_p',
        bind_vars={'p_0': 5},
        returns='o_p',
        result=Company
    )


def test_like():
    compare_query(
        cursor=TEST_DB.get(Company).match(like('some1', 'avocado%')),
        query_str='FOR o_p IN company'
                  '  FILTER o_p.some1 LIKE @p_0'
                  '  RETURN o_p',
        bind_vars={'p_0': 'avocado%'},
        returns='o_p',
        result=Company
    )


def test_multiple_operators():
    compare_query(
        cursor=TEST_DB.get(Company).match(like('some_string', 'avocado%'), gt('some_number', 42)).match(eq('some_value', 'value')),
        query_str='FOR o_p IN company'
                  '  FILTER o_p.some_string LIKE @p_0'
                  '  FILTER o_p.some_number > @p_1'
                  '  FILTER o_p.some_value == @p_2'
                  '  RETURN o_p',
        bind_vars={'p_0': 'avocado%', 'p_1': 42, 'p_2': 'value'},
        returns='o_p',
        result=Company
    )


def test_multipltes_matches_and_operators():
    compare_query(
        cursor=TEST_DB.get(Company).match(like('some_string', 'avocado%'), gt('some_number', 42), some_condition=True).match(
            eq('some_object.field', 'value'), another_value=[4, 5]),
        query_str='FOR o_p IN company'
                  '  FILTER o_p.some_string LIKE @p_0'
                  '  FILTER o_p.some_number > @p_1'
                  '  FILTER o_p.some_condition == @p_2'
                  '  FILTER o_p.some_object.field == @p_3'
                  '  FILTER o_p.another_value == @p_4'
                  '  RETURN o_p',
        bind_vars={'p_0': 'avocado%', 'p_1': 42, 'p_2': True, 'p_3': 'value', 'p_4': [4, 5]},
        returns='o_p',
        result=Company
    )
