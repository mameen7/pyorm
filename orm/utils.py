class Condition:
    operations_map = {
        'eq': '=',
        'lt': '<',
        'lte': '<=',
        'gt': '>',
        'gte': '>=',
        'in': 'IN'
    }

    def __init__(self, **kwargs):
        sql_format_parts = list()
        self.query_vars = list()
        for expr, value in kwargs.items():
            if '__' not in expr:
                expr += '__eq'
            field, operation_expr = expr.split('__')
            operation_str = self.operations_map[operation_expr]
            
            if isinstance(value, list):
                vars_list = value
                sql_format_parts.append(f'{field} {operation_str} ({", ".join(["%s"]*len(vars_list))})')
                self.query_vars += vars_list
            else:
                sql_format_parts.append(f'{field} {operation_str} %s')
                self.query_vars.append(value)
        self.sql_format = ' AND '.join(sql_format_parts)

    def __or__(self, other):
        return self._merge_with(other, logical_operator='OR')

    def __and__(self, other):
        return self._merge_with(other, logical_operator='AND')

    def _merge_with(self, other, logical_operator='AND'):
        condition_resulting = Condition()
        condition_resulting.sql_format = f"({self.sql_format}) {logical_operator} ({other.sql_format})"
        condition_resulting.query_vars = self.query_vars + other.query_vars
        return condition_resulting


class Field:
    def __init__(self, name, data_type):
        self.name = name
        self.data_type = data_type

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.name} ({self.data_type})>"
