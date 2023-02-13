from flask import Flask
from flask_migrate import Migrate
import os
from os import environ
from app.models import db, login, Company, UserModel, Task
from flask_login import current_user

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
app.config['ADMINISTRATOR_ROLE'] = 'administrator'
app.config['USER_ROLE'] = 'user'
app.config['NOBODY'] = 'nobody'
app.config['YANDEX_FOLDER'] = "C:\Yandex.Disk"
app.config['YANDEX_KEY_FILES_PATH'] = '/TASKER/KEY_FILES'
app.config['YANDEX_ALL_CARDS_WB'] = "/TASKER/ALL_CARDS_WB"
app.config['YANDEX_FOLDER_IMAGE'] = "C:\Yandex.Disk\ФОТОГРАФИИ\НОВЫЕ"
app.config['YANDEX_FOLDER_IMAGE_YANDISK'] = "/ФОТОГРАФИИ/НОВЫЕ"
app.config['NET_COST_PRODUCTS'] = "/TASKER/NET_COST"
app.config['CHARACTERS_PRODUCTS'] = "/TASKER/CHARACTERS"
app.config['COLORS'] = "/TASKER/CHARACTERS/COLORS"
app.config['ECO_FURS_WOMEN'] = "/TASKER/CHARACTERS/ECO_FURS_WOMEN"
app.config['MIT'] = "/TASKER/CHARACTERS/MIT"
app.config['APRON'] = "/TASKER/CHARACTERS/APRON"
app.config['SHOES'] = "/TASKER/CHARACTERS/SHOES"
app.config['SHOES'] = "/TASKER/CHARACTERS/SHOES"
app.config['SPEC_EXAMPLE'] = "/TASKER/SPEC_EXAMPLE"
app.config['PARTNERS_FOLDER'] = "ПОСТАВЩИКИ/TEST"
app.config['ARRIVALS_FOLDER'] = "Приходы"
app.config['ARRIVAL_FILE_NAMES'] = "Приход"
app.config['EXTENSION_EXCEL'] = ".xlsx"
app.config["FULL_PATH_ARRIVALS"] = \
    f"{app.config['YANDEX_FOLDER']}/{app.config['PARTNERS_FOLDER']}/*/{app.config['ARRIVALS_FOLDER']}/*/"
app.config["FULL_PATH_ARRIVALS_RECURSIVELY"] = \
    f"{app.config['YANDEX_FOLDER']}/{app.config['PARTNERS_FOLDER']}/*/{app.config['ARRIVALS_FOLDER']}/**/"
app.config['DAYS_STEP_DEFAULT'] = 15

db.init_app(app)
login.init_app(app)
login.login_view = 'login'
app.app_context().push()

# @app.before_first_request
# def create_all():
#     db.create_all()


with app.app_context():
    def create_all():
        db.create_all()

    if current_user:
        app.config['CURRENT_COMPANY_ID'] = current_user.company_id
        app.config['YANDEX_TOKEN'] = Company.query.filter_by(id=current_user.company_id).one().yandex_disk_token
        app.config['WB_API_TOKEN'] = Company.query.filter_by(id=current_user.company_id).one().wb_api_token
        app.config['WB_API_TOKEN2'] = Company.query.filter_by(id=current_user.company_id).one().wb_api_token2
    else:
        app.config['CURRENT_COMPANY_ID'] = 0
        app.config['YANDEX_TOKEN'] = 0
        app.config['WB_API_TOKEN'] = 0
        app.config['WB_API_TOKEN2'] = 0




# if current_user:
#     print(f"app.app_context current_user {current_user}")
#     current_user.company_id = current_user.company_id
#     app.config['YANDEX_TOKEN']  = Company.query.filter_by(id=current_user.company_id).one().yandex_disk_token
#     Company.query.filter_by(id=current_user.company_id).one().wb_api_token = Company.query.filter_by(id=current_user.company_id).one().wb_api_token
#     Company.query.filter_by(id=current_user.company_id).one().wb_api_token2 = Company.query.filter_by(id=current_user.company_id).one().wb_api_token2
#
# with app.app_context():
#     def create_all():
#         db.create_all()
#
#     if current_user:
#         current_user.company_id = current_user.company_id
#         app.config['YANDEX_TOKEN']  = Company.query.filter_by(id=current_user.company_id).one().yandex_disk_token
#         Company.query.filter_by(id=current_user.company_id).one().wb_api_token = Company.query.filter_by(id=current_user.company_id).one().wb_api_token
#         Company.query.filter_by(id=current_user.company_id).one().wb_api_token2 = Company.query.filter_by(id=current_user.company_id).one().wb_api_token2
#
#     else:
#         print(f"app.app_context current_user {current_user}")
#         current_user.company_id = 0
#         app.config['YANDEX_TOKEN']  = 0
#         Company.query.filter_by(id=current_user.company_id).one().wb_api_token = 0
#         Company.query.filter_by(id=current_user.company_id).one().wb_api_token2 = 0

from app.views import crop_images_views
from app.views import profile_views
from app.views import transactions_views
from app.views import tasks_views
from app.views import demand_calculation_views
from app.views import parser_rating_wb_views
from app.views import products_views
from app.views import yandex_disk_views
from app.views import detailing_report_views
from app.views import spec_views
from app.views import routes_getter_views
from app.views import images_foldering_views
from app.views import barcode_views
from app.views import data_transforming
from app.views import warehouse_views
from app.views import models_views
