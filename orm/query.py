from orm.utils import Condition

class Query:
    def __init__(self, table_name):
        self.table_name = table_name

    def get_filter_query(self, fields, limit=None, condition=None, **kwargs):
        if '*' in fields:
            fields_format = '*'
            # fields = [field.name for field in self._get_fields()]
        else:
            fields_format = ', '.join(fields)

        query = f"SELECT {fields_format} FROM {self.table_name}"
        params = []

        if kwargs and not condition:
            condition = Condition(**kwargs)

        if condition:
            query += f" WHERE {condition.sql_format}"
            params += condition.query_vars

        if limit:
            query += f" LIMIT %s"
            params.append(limit)

        return query, params
