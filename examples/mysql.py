from orm import db_connector
from orm import models
from orm.managers import MySQLManager
from orm.utils import Q


db_settings = {
    'host': '127.0.0.1', 
    'database': 'test_db', 
    'user': 'test_user', 
    'password': 'test_password'
}
db_server = 'mysql' 
db_connector.connect(db_server, db_settings)


class User(models.Model):
    table_name = 'users'
    model_manager = MySQLManager


# SQL: SELECT * FROM users WHERE id = 1;
user = User.objects.get(id=1)

# SQL: SELECT first_name, last_name FROM users WHERE id = 1;
user = User.objects.get(['first_name', 'last_name'], id=1)

# SQL: SELECT * FROM users;
users = User.objects.all()

# SQL: SELECT id, first_name, last_name FROM users;
users = User.objects.all(['id', 'first_name', 'last_name'])

# SQL: SELECT age FROM users WHERE id <= 3;
users = User.objects.filter(['age'], id__lte=3)
users = User.objects.filter(['age'], condition=Q(id__lte=3))

# SQL: SELECT age FROM users WHERE id >= 2 AND id <= 4;
users = User.objects.filter(['age'], id__gte=2, id__lte=4)
users = User.objects.filter(['age'], condition=Q(id__gte=2, id__lte=4))
users = User.objects.filter(['age'], condition=Q(id__gte=2) & Q(id__lte=4))

# SQL: SELECT age FROM users WHERE id < 2 OR id > 3;
users = User.objects.filter('salary', condition=Q(id__lt=2) | Q(id__gt=3))

# SQL: SELECT * FROM users WHERE first_name = last_name;
users = USer.objects.filter(condition=Q(first_name=F('last_name')))

# SQL: SELECT * FROM users WHERE age < id * 10;
users = User.objects.filter(condition=Q(age__lt=F('id')*10))

# SQL: SELECT * FROM users WHERE id IN (3, 4);
users = User.objects.filter(id__in=[3, 4])
users = User.objects.filter(condition=Q(id__in=[3, 4]))

# SQL: INSERT INTO users (first_name, last_name, age)
#          VALUES ('Ahmad', 'Ameen', 26)
# ;
User.objects.create(first_name="Ahmad", last_name="Ameen", age=26)

# SQL: UPDATE user SET age = 27, email = ahmadmameen7@gmail.com WHERE id > 1;
User.objects.update({'age': 27, 'email': 'ahmadmameen7@gmail.com'}, id=1)
user = User.objects.get(id=1)
user.age = 27
user.email = 'ahmadmameen7@gmail.com'
user.save()

# SQL DELETE FROM users WHERE age < 20;
User.objects.delete(age__lt=20)
User.objects.delete(condition=Q(age__lt=20))

# Get the list of the fields in your model
# It corresponds to the columns of the referenced table in database
Users.get_fields()
