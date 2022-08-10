from flask import Flask
from flask_migrate import Migrate
import os
from os import environ
from app.models import db, login, Company, UserModel, Task
from flask_login import login_required, current_user, login_user, logout_user, LoginManager

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
app.config['APP_NAME'] = 'TASKER'
app.config['ALLOWED_EXTENSIONS'] = ['.jpg', '.jpeg', '.png', '.gif', '.zip']
app.config['SQLALCHEMY_DATABASE_URI'] = uri or 'postgresql://postgres:19862814@localhost:8000/data'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['URL'] = 'https://cloud-api.yandex.net/v1/disk/resources'
app.config['ROLES'] = ['administrator', 'user', 'guest']
app.config['ADMINISTRATOR'] = 'administrator'
app.config['NOBODY'] = 'nobody'
app.config['YANDEX_KEY_FILES_PATH'] = '/TASKER/KEY_FILES'
app.config['YANDEX_FOLDER_IMAGE'] = "C:\Yandex.Disk\ФОТОГРАФИИ\НОВЫЕ"
app.config['YANDEX_FOLDER_IMAGE_YANDISK'] = "/ФОТОГРАФИИ/НОВЫЕ"

db.init_app(app)
login.init_app(app)
login.login_view = 'login'
app.app_context().push()


@app.before_first_request
def create_all():
    db.create_all()


# app key and tokens form db config
@app.before_first_request
def config():
    app.config['CURRENT_COMPANY_ID'] = Company.query.filter_by(id=current_user.company_id).one().id
    app.config['YANDEX_TOKEN'] = Company.query.filter_by(id=current_user.company_id).one().yandex_disk_token
    app.config['WB_API_TOKEN'] = Company.query.filter_by(id=current_user.company_id).one().wb_api_token
    app.config['WB_API_TOKEN2'] = Company.query.filter_by(id=current_user.company_id).one().wb_api_token2
    app.config['DAYS_STEP_DEFAULT'] = 7


from app.views import crop_images_views
from app.views import profile_views
from app.views import transactions_views
from app.views import tasks_views
from app.views import catalog_views
from app.views import parser_rating_wb_views
from app.views import products_views
from app.views import yandex_disk
from app.views import detailing_report_views
from app.views import spec_views
from app.views import routes_getter_views
from app.views import images_foldering_views
