from orm.exceptions import MissingParameter
from orm.managers.postgresql import PostgreSQLManager
from orm.managers.mysql import MySQLManager

def connect(db_server, db_settings):
    if not (db_server and db_settings):
        # raise exception
        raise MissingParameter('Missing Parameter(s): db_name and db_settings are required!')

    if db_server.lower() == 'postgresql':
        PostgreSQLManager.set_connection(db_settings)
    if db_server.lower() == 'mysql':
        MySQLManager.set_connection(db_settings)
