from app import app
from app.modules import parser_rating_module
from app.logging_config import setup_logging
from passwords import CURRENT_COMPANY_ID, YANDEX_TOKEN, WB_API_TOKEN

if __name__ == '__main__':
    with app.app_context():
        setup_logging()
        app.config['CURRENT_COMPANY_ID'] = CURRENT_COMPANY_ID
        app.config['YANDEX_TOKEN'] = YANDEX_TOKEN
        app.config['WB_API_TOKEN'] = WB_API_TOKEN
        parser_rating_module.batched_get_rating()
