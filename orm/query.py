import itertools
from orm.utils import Q

class Query:
    def __init__(self, table_name):
        self._table_name = table_name

    def get_filter_query(self, fields, condition, limit, **kwargs):
        fields_format = '*' if not fields else ', '.join(fields)
        query = f"SELECT {fields_format} FROM {self._table_name}"
        params = []
        if kwargs and not condition:
            condition = Q(**kwargs)
        if kwargs and condition:
            condition = condition & Q(**kwargs)
        if condition:
            query += f" WHERE {condition.sql_format}"
            params += condition.query_vars
        if limit:
            query += f" LIMIT %s"
            params.append(limit)

        return query, params

    def get_update_query(self, new_data, condition, **kwargs):
        temp_arr = []
        for field_name, value in new_data.items():
            if isinstance(value, int) or isinstance(value, float):
                temp_arr.append(f'{field_name} = {value}')
            else:
                temp_arr.append(f"{field_name} = '{value}'")

        new_data_format = ', '.join(temp_arr)
        query = f"UPDATE {self._table_name} SET {new_data_format}"
        params = []
        if kwargs and not condition:
            condition = Condition(**kwargs)
        if kwargs and condition:
            condition = condition & Q(**kwargs)
        if condition:
            query += f" WHERE {condition.sql_format}"
            params += condition.query_vars

        return query, params

    def get_bulk_create_query(self, data):
        fields_names = data[0].keys()
        values = []
        for row in data:
            assert row.keys() == fields_names
            values.append(tuple(row[field_name] for field_name in fields_names))

        # Build the insertion query
        n_fields, n_rows = len(fields_names), len(values)
        values_row_format = f'({", ".join(["%s"]*n_fields)})'
        values_format = ", ".join([values_row_format]*n_rows)

        fields_format = ', '.join(fields_names)
        query = f"INSERT INTO {self._table_name} ({fields_format}) VALUES {values_format}"
        params = tuple(itertools.chain(*values))

        return query, params

    def get_delete_query(self, condition, **kwargs):
        # Build DELETE query
        query = f"DELETE FROM {self._table_name} "
        params = []
        if kwargs and not condition:
            condition = Q(**kwargs)
        if condition:
            query += f" WHERE {condition.sql_format}"
            params += condition.query_vars

        return query, params
