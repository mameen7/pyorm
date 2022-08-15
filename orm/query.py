from orm.utils import Condition

class Query:
    def __init__(self, table_name):
        self.table_name = table_name

    def get_filter_query(self, fields, limit=None, condition=None, **kwargs):
        fields_format = '*' if '*' in fields else ', '.join(fields)
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

    def get_update_query(self, new_data, condition=None, **kwargs):
        temp_arr = []
        for field_name, value in new_data.items():
            if isinstance(value, int) or isinstance(value, float):
                temp_arr.append(f'{field_name} = {value}')
            else:
                temp_arr.append(f"{field_name} = '{value}'")

        new_data_format = ', '.join(temp_arr)
        query = f"UPDATE {self.table_name} SET {new_data_format}"
        params = []
        if kwargs and not condition:
            condition = Condition(**kwargs)
        if condition:
            query += f" WHERE {condition.sql_format}"
            params += condition.query_vars

        return query, params
