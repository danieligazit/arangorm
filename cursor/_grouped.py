from abc import ABC, abstractmethod
from typing import Dict

from _result import Result
from _stmt import Stmt


class Grouped(ABC):
    @abstractmethod
    def _to_group_stmt(self, prefix: str, collected, alias_to_result: Dict[str, Result]) -> Stmt:
        pass

