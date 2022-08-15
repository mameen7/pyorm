import psycopg2
from orm.managers.base import BaseManager


class PostgreSQLManager(BaseManager):
    connection = None

    @classmethod
    def set_connection(cls, db_settings):
        connection = psycopg2.connect(**db_settings)
        connection.autocommit = True
        cls.connection = connection
