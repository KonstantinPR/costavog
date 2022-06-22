from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import LoginManager
import os

uri = os.getenv("DATABASE_URL")  # or other relevant config var
if uri:
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
# rest of connection code using the connection string `uri`

login = LoginManager()
db = SQLAlchemy()


class UserModel(UserMixin, db.Model):
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    user_name = db.Column(db.String(100))
    user_email = db.Column(db.String(100))
    password_hash = db.Column(db.String(500))
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'))
    initial_sum = db.Column(db.Integer, default=0)
    initial_file_path = db.Column(db.String(500), default=0)
    role = db.Column(db.String(500), default='user')
    tasks = db.relationship('Task')
    points = db.Column(db.Integer, default=0)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


class Company(db.Model):
    __tablename__ = 'companies'

    id = db.Column(db.Integer, primary_key=True)
    company_name = db.Column(db.String(80), unique=False)
    password_hash = db.Column(db.String(500))
    users = db.relationship('UserModel')
    yandex_disk_token: object = db.Column(db.String(1000), default=0)
    wb_api_token = db.Column(db.String(1000), default=0)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


class Product(db.Model):
    __tablename__ = 'products'

    article = db.Column(db.String(80), primary_key=True)
    net_cost = db.Column(db.Integer)
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'))
    companies = db.relationship('Company')


class Transaction(db.Model):
    __tablename__ = 'transactions'

    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer)
    amount = db.Column(db.String(80), unique=False)
    description = db.Column(db.String(1000))
    user_name = db.Column(db.String(100))
    date = db.Column(db.String(100))
    yandex_link = db.Column(db.String(1000))


class Task(db.Model):
    __tablename__ = 'tasks'

    id = db.Column(db.Integer, primary_key=True)
    amount = db.Column(db.String(80), unique=False)
    description = db.Column(db.String(1000))
    user_name = db.Column(db.String(100))
    company_id = db.Column(db.Integer)
    date = db.Column(db.String(100))
    executor_id = db.Column(db.Integer, db.ForeignKey('users.id'))
    condition = db.Column(db.String(100))
    yandex_link = db.Column(db.String(1000))


@login.user_loader
def load_user(id):
    return UserModel.query.get(int(id))
