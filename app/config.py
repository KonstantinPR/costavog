from flask_login import current_user
from app import app
from app.models import Company


def set():
    print(f"Setting config for current_user {current_user}")
    if current_user:
        app.config['CURRENT_COMPANY_ID'] = current_user.company_id
        app.config['YANDEX_TOKEN'] = Company.query.filter_by(id=current_user.company_id).one().yandex_disk_token
        app.config['WB_API_TOKEN'] = Company.query.filter_by(id=current_user.company_id).one().wb_api_token
        app.config['WB_API_TOKEN2'] = Company.query.filter_by(id=current_user.company_id).one().wb_api_token2
        print(f"For current_user {current_user} config is updated")
    return None
