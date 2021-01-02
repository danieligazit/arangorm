from enum import Enum


class Operator(Enum):
    LIKE = 'LIKE'
    EQ = '=='
    NE = '!='
    LT = '<'
    LTE = '<='
    GT = '>'
    GTE = '>='

    def __repr__(self):
        return self.value

    def __str__(self):
        return self.value

