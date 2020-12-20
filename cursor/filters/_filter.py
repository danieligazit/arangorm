from abc import abstractmethod, ABC

from _stmt import Stmt


class Filter(ABC):
    @abstractmethod
    def _to_filter_stmt(self, prefix: str, relative_to: str) -> Stmt:
        pass
