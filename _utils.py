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


def hasattrribute(obj: Any, item: str) -> bool:
    try:
        obj.__getattribute__(item)
        return True
    except:
        return False
