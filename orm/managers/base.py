from orm.utils import Field, Q
from orm.query import Query
from orm.exceptions import MissingParameter, ObjectDoesNotExiet, InvalidParameter


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
    
    def filter(self, fields=None, condition=None, limit=None, **kwargs):
        if not (isinstance(fields, list) or isinstance(fields, Q)):
            raise InvalidParameter('InvalidParameter! Invalid query parameter')
        if isinstance(fields, Q):
            condition, fields = fields, None

        sql_query, params = self.query.get_filter_query(fields, condition, limit, **kwargs)
        cursor = self._get_cursor()
        cursor.execute(sql_query, params)

        return self._get_filter_query_result(cursor, fields)

    def get(self, fields=None, condition=None, **kwargs):
        if not (isinstance(fields, list) or isinstance(fields, Q)):
            raise InvalidParameter('InvalidParameter! Invalid query parameter')
        if isinstance(fields, Q):
            condition, fields = fields, None
        if not (condition or kwargs):
            raise MissingParameter('Error! At least one condition is required')

        model_object = self.filter(fields, condition=condition, limit=1, **kwargs)
        if not model_object:
            raise ObjectDoesNotExiet('Error! Object does not exit')

        return model_object[0]

    def update(self, new_data, condition=None, **kwargs):
        if not new_data:
            raise MissingParameter('Error! Missing update data')
        if not (condition or kwargs):
            raise MissingParameter('Error! At least one condition is required')
            
        # Ensure that the record exist in the database before executing update
        self.get(condition=condition, **kwargs)
        sql_query, params = self.query.get_update_query(new_data, condition, **kwargs)
        self._execute_query(sql_query, params)

    def create(self, **kwargs):
        self.bulk_create(data=[kwargs])

    def bulk_create(self, data):
        sql_query, params = self.query.get_bulk_create_query(data)
        self._execute_query(sql_query, params)

    def delete(self, condition=None, **kwargs):
        if not (condition or kwargs):
            raise MissingParameter('Error! At least one condition is required')
        self.get(condition=condition, **kwargs)
        sql_query, params = self.query.get_delete_query(condition, **kwargs)
        self._execute_query(sql_query, params)
