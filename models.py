from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import LoginManager

login = LoginManager()
db = SQLAlchemy()


class UserModel(UserMixin, db.Model):
    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True)
    user_name = db.Column(db.String(100))
    password_hash = db.Column(db.String(500))
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'))

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


class Post(db.Model):
    __tablename__ = 'posts'

    id = db.Column(db.Integer, primary_key=True)
    amount = db.Column(db.String(80), unique=False)
    description = db.Column(db.String(500))
    user_name = db.Column(db.String(100))
    date = db.Column(db.String(100))


class Task(db.Model):
    __tablename__ = 'tasks'

    id = db.Column(db.Integer, primary_key=True)
    amount = db.Column(db.String(80), unique=False)
    description = db.Column(db.String(100))
    username = db.Column(db.String(100))
    date = db.Column(db.String(100))


@login.user_loader
def load_user(id):
    return UserModel.query.get(int(id))
