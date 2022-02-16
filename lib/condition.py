def get_or_bound(text_or_condition):
    if type(text_or_condition) == str:
        return text_or_condition
    else:
        return f'({text_or_condition.__str__()})'


class Condition:

    def __init__(self, *operands, operator='AND'):
        self._operands = operands
        self._operator = operator

    @property
    def operands(self):
        return self._operands

    @property
    def operator(self):
        return self._operator

    def combine(self, operator=None):
        return Condition(operator, self.operands)

    def get_string(self):
        bounded_operands = [get_or_bound(o) for o in self.operands]
        return f' {self.operator} '.join(bounded_operands)

    def __str__(self):
        return self.get_string()

