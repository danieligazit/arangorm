from dataclasses import dataclass
from _query import Returns


@dataclass
class Var(Returns):
    _name: str

    # def _to_group_stmt(self, prefix: str, alias_to_result: Dict[str, Result], collected: str = 'groups') -> Stmt:
    #     return Stmt(f'''{self._name}{self.attribute_return}''', {},
    #                 result=self._get_result(alias_to_result[self._name]))
    #
    # def _to_select_stmt(self, prefix: str, alias_to_result: Dict[str, Result], relative_to: str = '') -> Stmt:
    #     return Stmt(f'''{self._name}{self.attribute_return}''', {},
    #                 result=self._get_result(alias_to_result[self._name]))


def var(expression: str):
    v = Var(_name=expression)
    return v
