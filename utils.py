import re
from typing import Tuple, Dict, Any


class classproperty():
    def __init__(self, f):
        self.__f = f

    def __get__(self, obj, owner):
        if obj:
            return self.__f(obj)
        return self.__f(owner)


def convert_camel_to_snake(name):
    name = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def returns(return_entity: str):
    def decorator(func):
        def inner_function(self, prefix: str = 'p', with_return: bool = False) -> Tuple[str, Dict[str, Any]]:
            stmt, bind_vars = func(self, prefix=prefix, with_return=with_return)

            if with_return:
                return stmt + f' RETURN {return_entity}', bind_vars

            return stmt, bind_vars

        return inner_function

    return decorator
