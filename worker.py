from app import set_config_scheduler, app
from app.modules import parser_rating_module
from app.logging_config import setup_logging

def run_batched_get_rating():
    with app.app_context():
        setup_logging()
        set_config_scheduler()
        parser_rating_module.batched_get_rating()


if __name__ == '__main__':
    run_batched_get_rating()