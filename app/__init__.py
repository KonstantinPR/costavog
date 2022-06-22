import flask
from flask import Flask
from flask_migrate import Migrate
import os
from os import environ
from app.models import db, login, Company, UserModel, Task
from flask_login import login_required, current_user, login_user, logout_user

app = Flask(__name__)
migrate = Migrate(app, db)
app.secret_key = 'xyz1b9zs8erh8be1g8-vw4-1be89ts4er1v'

#  to solve problems connection with SQLAlchemy > 1.4 in heroku
uri_old = os.getenv("DATABASE_URL")  # or other relevant config var
uri = environ.get('DATABASE_URL')
if uri:
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)

# app config
app.config['ALLOWED_EXTENSIONS'] = ['.jpg', '.jpeg', '.png', '.gif', '.zip']
app.config['SQLALCHEMY_DATABASE_URI'] = uri or 'postgresql://postgres:19862814@localhost:8000/data'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# app.config['YANDEX_TOKEN'] = Company.query.filter_by(id=current_user.company_id).one().yandex_disk_token

# yandex token is placed in db (company.yandex_token)

db.init_app(app)
app.app_context().push()
login.init_app(app)
login.login_view = 'login'

# api, keys and const config
app.config['URL'] = 'https://cloud-api.yandex.net/v1/disk/resources'

app.config['YANDEX_TOKEN'] = Company.query.filter_by(id=1).one().yandex_disk_token


from app.views import crop_images_views
from app.views import profile_views
from app.views import transactions_views
from app.views import tasks_views
from app.views import catalog_views
from app.views import parser_rating_wb_views
from app.views import products_views
from app.views import yandex_disk
