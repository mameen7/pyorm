import psycopg2
import mysql.connector

from orm.utils import Field
from orm.query import Query
from orm.exceptions import MissingParameter, ObjectDoesNotExiet


class BaseManager:
    connection = None

    def __init__(self, model_class):
        self.model_class = model_class

    @classmethod
    def set_pg_connection(cls, db_settings):
        connection = psycopg2.connect(**db_settings)
        connection.autocommit = True
        cls.connection = connection

    @classmethod
    def set_mysql_connection(cls, db_settings):
        connection = mysql.connector.connect(**db_settings)
        cls.connection = connection

    def _get_cursor(self):
        return self.connection.cursor()

    def _execute_query(self, query, params):
        cursor = self._get_cursor()
        cursor.execute(query, params)
        
    @property
    def table_name(self):
        return self.model_class.table_name

    def _get_fields(self):
        cursor = self._get_cursor()
        cursor.execute(
            """
            SELECT column_name, data_type FROM information_schema.columns WHERE table_name=%s
            """,
            (self.table_name, )
        )

        return (Field(name=row[0], data_type=row[1]) for row in cursor.fetchall())
    
    def _get_filter_query_result(self, cursor, fields):
        # The fetching is done in batches to avoid memory run out.
        if '*' in fields:
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
    
    def filter(self, fields='*', limit=None, condition=None, **kwargs):
        # Get generated sql query with parameters
        query, params = Query(self.table_name).get_filter_query(fields, limit, condition, **kwargs)
        # Execute query
        cursor = self._get_cursor()
        cursor.execute(query, params)

        return self._get_filter_query_result(cursor, fields)

    def get(self, fields='*', condition=None, **kwargs):
        if not (condition or kwargs):
            raise MissingParameter('Error! At least one condition is required')
        model_object = self.filter(fields, limit=1, condition=condition, **kwargs)
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
        query, params = query, params = Query(self.table_name).get_update_query(new_data, condition, **kwargs)
        self._execute_query(query, params)
