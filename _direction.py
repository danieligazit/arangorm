from enum import Enum


class Direction(Enum):
    INBOUND = 'INBOUND'
    OUTBOUND = 'OUTBOUND'

    def __repr__(self):
        return self.value

    def __str__(self):
        return self.value



