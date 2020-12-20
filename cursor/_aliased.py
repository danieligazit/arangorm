from dataclasses import field, dataclass
from typing import List


@dataclass
class Aliased:
    aliases: List[str] = field(default_factory=list, init=False)

    def as_var(self, variable: str) -> Q:
        self.aliases.append(variable)
        return self
