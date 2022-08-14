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
        # The fetching is done by batches to avoid memory run out.
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
    
    def filter(self, fields, limit=None, condition=None, **kwargs):
        if not fields:
            # raise exception
            raise MissingParameter('Missing Parameter! Filter is missing a required argument fields: []')
        
        # Get generated sql query with parameters
        query, params = Query(self.table_name).get_filter_query(fields, limit, condition, **kwargs)

        # Execute query
        cursor = self._get_cursor()
        cursor.execute(query, params)

        return self._get_filter_query_result(cursor, fields)

    def get(self, fields, **kwargs):
        if not kwargs:
            raise MissingParameter('At least one condition is required!')
        model_object = self.filter(fields, **kwargs)
        if not model_object:
            raise ObjectDoesNotExiet('Object does not exit!')
        return model_object[0]
