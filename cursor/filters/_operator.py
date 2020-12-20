from enum import Enum


class Operator(Enum):
    Like = 'LIKE'
    EQUALS = '=='
    GT = '>='

    def __repr__(self):
        return self.name
