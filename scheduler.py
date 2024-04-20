from app import set_config_schedule, app
from app.modules import parser_rating_module
from app.logging_config import setup_logging

if __name__ == '__main__':
    with app.app_context():
        setup_logging()
        set_config_schedule()
        parser_rating_module.scheduled_get_rating()
