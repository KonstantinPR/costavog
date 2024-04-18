from flask import Flask, session, redirect
from flask_migrate import Migrate
import os
from os import environ
from app.models import db, login, Company, UserModel
from flask_login import LoginManager, current_user
import time

app = Flask(__name__)
app.secret_key = 'xyz1b9zs8erh8be1g8-vw4-1be89ts4er1v'
login_manager = LoginManager()
login_manager.init_app(app)
login.init_app(app)
migrate = Migrate(app, db)

#  to solve problems connection with SQLAlchemy > 1.4 in heroku
uri = environ.get('DATABASE_URL')

if uri:
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)

# app config

app.config['APP_PASSWORD'] = '19862814GVok'
app.config['APP_NAME'] = 'TASKER'
app.config['ALLOWED_EXTENSIONS'] = ['.jpg', '.jpeg', '.png', '.gif', '.zip']
app.config['SQLALCHEMY_DATABASE_URI'] = uri
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['URL'] = 'https://cloud-api.yandex.net/v1/disk/resources'
app.config['ROLES'] = ['administrator', 'user', 'guest']
app.config['ADMINISTRATOR_ROLE'] = 'administrator'
app.config['USER_ROLE'] = 'user'
app.config['NOBODY'] = 'nobody'
app.config['YANDEX_FOLDER'] = "C:\YandexDisk"
app.config['TMP_IMG_FOLDER'] = "folder_img"
app.config['YANDEX_PATH'] = ''
app.config['YANDEX_KEY_FILES_PATH'] = '/TASKER/KEY_FILES'
app.config['YANDEX_KEY_STORAGE_COST'] = '/TASKER/KEY_FILES/STORAGE_COST'
app.config['YANDEX_KEY_STOCK_WB'] = '/TASKER/KEY_FILES/STOCK_WB'
app.config['YANDEX_KEY_PRICES'] = '/TASKER/KEY_FILES/PRICES'
app.config['YANDEX_ALL_CARDS_WB'] = "/TASKER/ALL_CARDS_WB"
app.config['YANDEX_SALES_FUNNEL_WB'] = "/TASKER/SALES_FUNNEL"
app.config['YANDEX_FOLDER_IMAGE'] = "C:\YandexDisk\ФОТОГРАФИИ"
app.config['YANDEX_FOLDER_IMAGE_YANDISK'] = "/ФОТОГРАФИИ"
app.config['NET_COST_PRODUCTS'] = "/TASKER/NET_COST"
app.config['RATING'] = "/TASKER/RATING"

app.config['CHARACTERS_PRODUCTS'] = "/TASKER/CHARACTERS"
app.config['COLORS'] = "/TASKER/CHARACTERS/COLORS"
app.config['ECO_FURS_WOMEN'] = "/TASKER/CHARACTERS/ECO_FURS_WOMEN"
app.config['TIE'] = "/TASKER/CHARACTERS/TIE"
app.config['MIT'] = "/TASKER/CHARACTERS/MIT"
app.config['APRON'] = "/TASKER/CHARACTERS/APRON"
app.config['SHOES'] = "/TASKER/CHARACTERS/SHOES"
app.config['SHOES'] = "/TASKER/CHARACTERS/SHOES"
app.config['JEANS'] = "/TASKER/CHARACTERS/JEANS"
app.config['DEFAULT'] = "/TASKER/CHARACTERS/DEFAULT"

app.config['SPEC_EXAMPLE'] = "/TASKER/SPEC_EXAMPLE"
app.config['PARTNERS_FOLDER'] = "ПОСТАВЩИКИ/TEST"
app.config['ARRIVALS_FOLDER'] = "Приходы"
app.config['ARRIVAL_FILE_NAMES'] = "Приход"
app.config['EXTENSION_EXCEL'] = ".xlsx"
app.config["FULL_PATH_ARRIVALS"] = \
    f"{app.config['YANDEX_FOLDER']}/{app.config['PARTNERS_FOLDER']}/*/{app.config['ARRIVALS_FOLDER']}/*/"
app.config["FULL_PATH_ARRIVALS_RECURSIVELY"] = \
    f"{app.config['YANDEX_FOLDER']}/{app.config['PARTNERS_FOLDER']}/*/{app.config['ARRIVALS_FOLDER']}/**/"
app.config['DAYS_STEP_DEFAULT'] = 14
app.config['DAYS_PERIOD_DEFAULT'] = 1
app.config['LAST_DAYS_DEFAULT'] = 7

db.init_app(app)
login.init_app(app)
login.login_view = 'login'


def set_config():
    print(f"Setting config for current_user {current_user}")
    start_time = time.time()
    company = Company.query.filter_by(id=current_user.company_id).first()
    print(company)
    app.config['CURRENT_COMPANY_ID'] = company.id
    app.config['YANDEX_TOKEN'] = company.yandex_disk_token
    app.config['WB_API_TOKEN'] = company.wb_api_token
    app.config['WB_API_TOKEN2'] = company.wb_api_token2
    end_time = time.time()
    # Calculate the elapsed time in seconds
    elapsed_time = end_time - start_time
    print(f"For current_user {current_user} config is updated")
    print(f"Time querys to database to set app.config is {elapsed_time:.9f} seconds.")


@login_manager.request_loader
def load_user_from_request(request):
    user_id = request.headers.get('User-ID')
    print(f"request.headers.get('User-ID') {request.headers.get('User-ID')}")
    if user_id:
        return UserModel.query.get(user_id)
    return None


@app.before_request
def before_request():
    print(f'current_user.id {current_user.id}')
    if current_user.is_authenticated:
        set_config()
    return None


@app.before_first_request
def create_all():
    db.create_all()


@login_manager.user_loader
def load_user(user_id):
    return UserModel.query.get(user_id)


@login.user_loader
def load_user(id):
    return UserModel.query.get(int(id))


from app.views import crop_images_views
from app.views import profile_views
from app.views import transactions_views
from app.views import tasks_views
from app.views import demand_calculation_views
from app.views import parser_rating_wb_views
from app.views import products_views
from app.views import yandex_disk_views
from app.views import detailing_upload_views
from app.views import detailing_api_views
from app.views import spec_views
from app.views import routes_getter_views
from app.views import images_foldering_views
from app.views import barcode_views
from app.views import data_transforming
from app.views import warehouse_views
from app.views import models_views
from app.views import youtube_views
from app.views import api_views
