from dataclasses import field, dataclass
from typing import List, TypeVar

A = TypeVar('A', bound='Aliased')


@dataclass
class Aliased:
    aliases: List[str] = field(default_factory=list, init=False)

    def as_var(self, variable: str) -> A:
        self.aliases.append(variable)
        return self
