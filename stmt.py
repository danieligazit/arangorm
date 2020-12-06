from dataclasses import field, dataclass
from typing import Any, Dict, List, Tuple

from result import Result


@dataclass
class Stmt:
    query_str: str
    bind_vars: Dict[str, Any]
    aliases: List[str] = field(default_factory=list)
    returns: str = field(default=None)
    result: Result = field(default=None)
    alias_to_result: Dict[str, Result] = field(default_factory=dict)

    def __post_init__(self):
        for alias in self.aliases:
            self.alias_to_result[alias] = self.result

    def expand(self) -> Tuple[str, Dict[str, Any]]:
        if not self.returns:
            return self.expand_without_return()
        return self.query_str + f' RETURN {self.returns}', self.bind_vars

    def expand_without_return(self) -> Tuple[str, Dict[str, Any]]:
        return self.query_str, self.bind_vars
