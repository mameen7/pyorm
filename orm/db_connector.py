from orm.manager import BaseManager
from orm.exceptions import MissingParameter

def connect(db_name, db_settings):
    if not (db_name and db_settings):
        # raise exception
        raise MissingParameter('Missing Parameter(s): db_name and db_settings are required!')

    if db_name.lower() == 'postgresql':
        BaseManager.set_pg_connection(db_settings)
    if db_name.lower() == 'mysql':
        BaseManager.set_mysql_connection(db_settings)
