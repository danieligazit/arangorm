from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict

from _result import Result
from _stmt import Stmt


@dataclass
class Selected(ABC):
    @abstractmethod
    def _to_select_stmt(self, prefix: str, relative_to: str, alias_to_result: Dict[str, Result]) -> Stmt:
        pass
