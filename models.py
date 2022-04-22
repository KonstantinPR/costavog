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
    password_hash = db.Column(db.String(500))
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'))
    initial_sum = db.Column(db.Integer, default=0)

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

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


class Transaction(db.Model):
    __tablename__ = 'transactions'

    id = db.Column(db.Integer, primary_key=True)
    amount = db.Column(db.String(80), unique=False)
    description = db.Column(db.String(500))
    user_name = db.Column(db.String(100))
    company_id = db.Column(db.Integer)
    date = db.Column(db.String(100))


class Task(db.Model):
    __tablename__ = 'tasks'

    id = db.Column(db.Integer, primary_key=True)
    amount = db.Column(db.String(80), unique=False)
    description = db.Column(db.String(100))
    user_name = db.Column(db.String(100))
    company_id = db.Column(db.Integer)
    date = db.Column(db.String(100))


@login.user_loader
def load_user(id):
    return UserModel.query.get(int(id))
