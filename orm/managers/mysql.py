import mysql.connector
from orm.managers.base import BaseManager


class MySQLManager(BaseManager):
    connection = None

    @classmethod
    def set_mysql_connection(cls, db_settings):
        connection = mysql.connector.connect(**db_settings)
        cls.connection = connection
