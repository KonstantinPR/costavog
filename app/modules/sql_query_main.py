from app.models import UserModel
import logging
from app import app
from flask_login import current_user


def get_unique_users_company():
    if hasattr(current_user, 'company_id'):
        print(current_user)
        all_users = UserModel.query.filter_by(company_id=current_user.company_id).all()
        all_unique_users = [user.user_name for user in all_users]
        return all_unique_users
    return None
