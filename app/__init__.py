import logging
from flask import Flask
from flask_migrate import Migrate
from os import environ
from app.models import db, login, Company, UserModel
from flask_login import LoginManager, current_user
from dotenv import load_dotenv

# Initialize the Flask app
app = Flask(__name__)
app.logger.setLevel(logging.INFO)
app.secret_key = 'xyz1b9zs8erh8be1g8-vw4-1be89ts4er1v'

# Initialize LoginManager
login_manager = LoginManager()
login_manager.init_app(app)

# Load environment variables from .env file
load_dotenv()
app.config['ENVIRONMENT'] = environ.get('ENVIRONMENT')


# Context processor to inject environment variable into templates
@app.context_processor
def inject_environment():
    return {'ENVIRONMENT': app.config['ENVIRONMENT']}


# Initialize Flask-Migrate
migrate = Migrate(app, db)

# Setup database URI for SQLAlchemy
uri = environ.get('DATABASE_URL')
if uri and uri.startswith("postgres://"):
    uri = uri.replace("postgres://", "postgresql://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = uri
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize the database
db.init_app(app)


# Set up logging
@app.before_request
def before_request():
    logging.warning('Before request hook triggered.')
    set_config()  # Set the configuration for the current user
    return None


# Load user from request
@login_manager.request_loader
def load_user_from_request(request):
    user_id = request.headers.get('User-ID')
    logging.warning(f"User-ID from request: {user_id}")
    if user_id:
        return UserModel.query.get(user_id)
    return None


# Load user by ID
@login_manager.user_loader
def load_user(user_id):
    return UserModel.query.get(user_id)


# Set up app configuration based on the current user
def set_config():
    if current_user.is_authenticated:
        company = Company.query.filter_by(id=current_user.company_id).first()
        if company:
            app.config.update({
                'CURRENT_COMPANY_ID': company.id,
                'YANDEX_TOKEN': company.yandex_disk_token,
                'WB_API_TOKEN': company.wb_api_token,
                'WB_API_TOKEN2': company.wb_api_token2,
                'OZON_CLIENT_ID': company.ozon_client_id,
                'OZON_API_TOKEN': company.ozon_api_token,
                'TELEGRAM_BOT_TOKEN': company.telegram_token,
                'TELEGRAM_CHAT_ID': company.telegram_chat_id,
                'WB_DELIVERING': company.wb_delivery_token,
            })
        else:
            app.logger.warning(f"Company not found for user ID: {current_user.id}")
    else:
        app.logger.warning('User is not authenticated.')


@app.before_first_request
def create_all():
    db.create_all()


# Main block to configure app when run directly
app_config_dict = {
    'WB_DELIVERING': 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2NDAzNDM2NiwiaWQiOiIwMTk3MGNjYi1iNjdiLTdiNjAtOGU5ZC1lMjNjYjdiM2U0MWQiLCJpaWQiOjE2NDgwMDUwLCJvaWQiOjI5MDc3LCJzIjoxMDI0LCJzaWQiOiI1MTdiYzY5NC1kMzc1LTU4MTItOWEyZS0wNzk0ZDU5NmI2YTkiLCJ0IjpmYWxzZSwidWlkIjoxNjQ4MDA1MH0.CfPnsyDzhkpTYIoC3tiVKR_bzqrq1P9kQn7kNw_wv8dSjLPO-BKXMLuki8X0sCobJr34FuUIEXLQKN9kucklnQ',
    'TELEGRAM_CHAT_ID': '1495209774',
    'TELEGRAM_BOT_TOKEN': '8000171417:AAH1xX6puEuOftokCqWkCTuChv1wMpmKcSo',
    'APP_NAME': 'TASKER',
    'ALLOWED_EXTENSIONS': ['.jpg', '.jpeg', '.png', '.gif', '.zip'],
    'SQLALCHEMY_DATABASE_URI': uri,
    'SQLALCHEMY_TRACK_MODIFICATIONS': False,
    'URL': 'https://cloud-api.yandex.net/v1/disk/resources',
    'ROLES': ['administrator', 'user', 'guest'],
    'ADMINISTRATOR_ROLE': 'administrator',
    'USER_ROLE': 'user',
    'NOBODY': 'nobody',
    'YANDEX_FOLDER': "C:\YandexDisk",
    'TMP_IMG_FOLDER': "folder_img",
    'YANDEX_PATH': '',
    'YANDEX_KEY_FILES_PATH': '/TASKER/KEY_FILES',
    'YANDEX_KEY_STORAGE_COST': '/TASKER/KEY_FILES/STORAGE_COST',
    'YANDEX_KEY_STOCK_WB': '/TASKER/KEY_FILES/STOCK_WB',
    'YANDEX_KEY_PRICES': '/TASKER/KEY_FILES/PRICES',
    'REPORT_SALES_REALIZATION': '/TASKER/KEY_FILES/REPORT_SALES_REALIZATION',
    'REPORT_DETAILING_UPLOAD': '/TASKER/KEY_FILES/REPORT_DETAILING_UPLOAD',
    'REPORT_DETAILING_UPLOAD_LONG': '/TASKER/KEY_FILES/REPORT_DETAILING_UPLOAD/LONG',
    'REPORT_DETAILING_UPLOAD_ALL': '/TASKER/KEY_FILES/REPORT_DETAILING_UPLOAD/ALL_LONG',
    'REPORT_DETAILING_UPLOAD_2023': '/TASKER/KEY_FILES/REPORT_DETAILING_UPLOAD/2023',
    'REPORT_DETAILING_UPLOAD_2024': '/TASKER/KEY_FILES/REPORT_DETAILING_UPLOAD/2024',
    'REPORT_DETAILING_UPLOAD_2025': '/TASKER/KEY_FILES/REPORT_DETAILING_UPLOAD/2025',
    'REPORT_DYNAMIC': '/TASKER/KEY_FILES/DYNAMIC_REVENUE',
    'REPORT_WB_ZIPS': '/TASKER/KEY_FILES/ZIPS',
    'YANDEX_ALL_CARDS_WB': "/TASKER/KEY_FILES/ALL_CARDS_WB",
    'YANDEX_EXCLUDE_CARDS': "/TASKER/KEY_FILES/ALL_CARDS_WB/EXCLUDE_CARDS",
    'YANDEX_SALES_FUNNEL_WB': "/TASKER/KEY_FILES/SALES_FUNNEL",
    'YANDEX_FOLDER_IMAGE': "C:\YandexDisk\ФОТОГРАФИИ",
    'YANDEX_FOLDER_IMAGE_YANDISK': "/ФОТОГРАФИИ",
    'NET_COST_PRODUCTS': "/TASKER/KEY_FILES/NET_COST",
    'DELIVERY_PRODUCTS': r'C:\YandexDisk\СЕТЕВЫЕ МАГАЗИНЫ\WILDBERRIES\ОТПРАВКИ',
    'RATING': "/TASKER/KEY_FILES/RATING",

    # OZON
    'YANDEX_CARDS_OZON': '/TASKER/OZON/CARDS',
    'YANDEX_STOCK_OZON': '/TASKER/OZON/STOCK',
    'YANDEX_PRICE_OZON': '/TASKER/OZON/PRICE',
    'YANDEX_TRANSACTION_OZON': '/TASKER/OZON/TRANSACTION',
    'YANDEX_UPDATED_PRICES': '/TASKER/OZON/df_updated_prices',
    'YANDEX_CANDIDATES_FOR_ACTION': '/TASKER/OZON/df_candidates_for_action',

    'CHARACTERS_PRODUCTS': "/TASKER/CHARACTERS",
    'COLORS': "/TASKER/CHARACTERS/COLORS",
    'ECO_FURS_WOMEN': "/TASKER/CHARACTERS/ECO_FURS_WOMEN",
    'CLOTHES_TEMPLATE': "/TASKER/CHARACTERS/ECO_FURS_WOMEN/TEMPLATE_CLOTHES",
    'TIE': "/TASKER/CHARACTERS/TIE",
    'MIT': "/TASKER/CHARACTERS/MIT",
    'APRON': "/TASKER/CHARACTERS/APRON",
    'SHOES': "/TASKER/CHARACTERS/SHOES",
    'JEANS': "/TASKER/CHARACTERS/JEANS",
    'DEFAULT': "/TASKER/CHARACTERS/DEFAULT",

    'SPEC_EXAMPLE': "/TASKER/SPEC_EXAMPLE",
    'PARTNERS_FOLDER': "ПОСТАВЩИКИ/TEST",
    'ARRIVALS_FOLDER': "Приходы",
    'ARRIVAL_FILE_NAMES': "Приход",
    'EXTENSION_EXCEL': ".xlsx",
    'DAYS_STEP_DEFAULT': 7,
    'DAYS_DELAY_REPORT': 1,
    'DAYS_PERIOD_FUNNEL': 28,
    'DAYS_PERIOD_DEFAULT': 1,
    'LAST_DAYS_DEFAULT': 7,

}

# Update the app configuration

app.config.update(app_config_dict)

app_config_dict["FULL_PATH_ARRIVALS"] = \
    f"{app.config['YANDEX_FOLDER']}/{app.config['PARTNERS_FOLDER']}/*/{app.config['ARRIVALS_FOLDER']}/*/"
app_config_dict["FULL_PATH_ARRIVALS_RECURSIVELY"] = \
    f"{app.config['YANDEX_FOLDER']}/{app.config['PARTNERS_FOLDER']}/*/{app.config['ARRIVALS_FOLDER']}/**/"

if __name__ == "__main__":
    print("Setting in configuration...")

# Import views at the end to avoid circular imports
from app.views import (
    crop_images_views,
    profile_views,
    transactions_views,
    tasks_views,
    demand_calculation_views,
    parser_rating_wb_views,
    products_views,
    yandex_disk_views,
    detailing_upload_views,
    spec_views,
    routes_getter_views,
    images_foldering_views,
    barcode_views,
    data_transforming,
    warehouse_views,
    api_views,
    sales_funnel_views,
    deliveries_goods_views,
    wb_store_notification_views,
    api_actions_OZON_views,
)
