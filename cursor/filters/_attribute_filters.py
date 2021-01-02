from typing import Any
from cursor.filters._attribute_filter import AttributeFilter


def like(attribute: str, compare_value: Any) -> AttributeFilter:
    return AttributeFilter(attribute=attribute, operator='LIKE', compare_value=compare_value)


def eq(attribute: str, compare_value: Any) -> AttributeFilter:
    return AttributeFilter(attribute=attribute, operator='==', compare_value=compare_value)


def gt(attribute: str, compare_value: Any) -> AttributeFilter:
    return AttributeFilter(attribute=attribute, operator='>', compare_value=compare_value)