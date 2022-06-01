from flask import Flask
from flask_migrate import Migrate
import os
from os import environ
from app.models import db, login

app = Flask(__name__)
migrate = Migrate(app, db)
app.secret_key = 'xyz1b9zs8erh8be1g8-vw4-1be89ts4er1v'

#  to solve problems connection with SQLAlchemy > 1.4 in heroku
uri_old = os.getenv("DATABASE_URL")  # or other relevant config var
uri = environ.get('DATABASE_URL')
if uri:
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)

app.config['ALLOWED_EXTENSIONS'] = ['zip']
app.config['SQLALCHEMY_DATABASE_URI'] = uri or 'postgresql+psycopg2://postgres:19862814@localhost/data'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['URL'] = 'https://cloud-api.yandex.net/v1/disk/resources'
app.config['TOKEN'] = '' # here need Yandex TOKEN for API work

db.init_app(app)

login.init_app(app)
login.login_view = 'login'

from app.views import views
from app.views import profile_views
from app.views import transactions_views
from app.views import tasks_views
from app.views import catalog_views
from app.views import parser_rating_wb_views
from app.views import products_views
from app.views import yandex_disk
