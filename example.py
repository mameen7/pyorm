from orm import db_connector
from orm.models import Model
from orm.managers import PostgreSQLManager


db_settings = {'host': '127.0.0.1', 'database': 'tenant_subfolder_tutorial', 'user': 'tenant_subfolder_tutorial', 'password': 'qwerty'}

db_server = 'postgresql'
db_connector.connect(db_server, db_settings)
class User(Model):
    table_name = 'customers_client'
    model_manager = PostgreSQLManager
    
    
#User.objects.create(schema_name='jofay', name='Jopay', description='jofay description', created_on='2022-08-15')

users = User.objects.all()
print('---------------------------------------!!!!')
print(users)

User.objects.delete(schema_name='jofay')

users = User.objects.all()
print('---------------------------------------After')
print(users)
