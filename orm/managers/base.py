from orm.utils import Field, Q
from orm.query import Query
from orm.exceptions import ObjectDoesNotExist
from orm.decorators import (
    validate_filter_params, 
    validate_get_params,
    validate_update_params, 
    validate_create_data,
    validate_delete_params
)


class BaseManager:
    connection = None

    def __init__(self, model_class):
        self.model_class = model_class
        self.query = Query(self._table_name)

    def _get_cursor(self):
        return self.connection.cursor()

    def _execute_query(self, query, params):
        cursor = self._get_cursor()
        cursor.execute(query, params)
        
    @property
    def _table_name(self):
        return self.model_class.table_name

    def _get_fields(self):
        cursor = self._get_cursor()
        cursor.execute(
            """
            SELECT column_name, data_type FROM information_schema.columns WHERE table_name=%s
            """,
            (self._table_name, )
        )
        return (Field(name=row[0], data_type=row[1]) for row in cursor.fetchall())
    
    def _get_filter_query_result(self, cursor, fields):
        # The fetching is done in batches to avoid memory run out.
        if not fields:
            fields = [field.name for field in self._get_fields()]

        batch_size = 1000
        model_objects = []
        is_fetching_completed = False
        while not is_fetching_completed:
            rows = cursor.fetchmany(size=batch_size)
            for row in rows:
                row_data = dict(zip(fields, row))
                model_objects.append(self.model_class(**row_data))
            is_fetching_completed = len(rows) < batch_size

        return model_objects

    def all(self):
        return self.filter()
    
    @validate_filter_params
    def filter(self, fields=None, condition=None, limit=None, **kwargs):
        sql_query, params = self.query.get_filter_query(fields, condition, limit, **kwargs)
        cursor = self._get_cursor()
        cursor.execute(sql_query, params)
        return self._get_filter_query_result(cursor, fields)

    @validate_get_params
    def get(self, fields=None, condition=None, **kwargs):
        model_object = self.filter(fields, condition, limit=1, **kwargs)
        if not model_object:
            raise ObjectDoesNotExist
        return model_object[0]

    @validate_update_params
    def update(self, data, condition=None, **kwargs):
        # Ensure that the record exist in the database before executing update
        self.get(condition=condition, **kwargs)
        sql_query, params = self.query.get_update_query(data, condition, **kwargs)
        self._execute_query(sql_query, params)

    def create(self, **kwargs):
        self.bulk_create([kwargs])

    @validate_create_data
    def bulk_create(self, data):
        sql_query, params = self.query.get_bulk_create_query(data)
        self._execute_query(sql_query, params)

    @validate_delete_params
    def delete(self, condition=None, **kwargs):
        self.get(condition=condition, **kwargs)
        sql_query, params = self.query.get_delete_query(condition, **kwargs)
        self._execute_query(sql_query, params)
