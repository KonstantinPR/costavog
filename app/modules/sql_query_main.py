from app.models import UserModel
from app import app



def get_unique_users_company():
    all_users = UserModel.query.filter_by(company_id=app.config['CURRENT_COMPANY_ID']).all()
    all_unique_users = [user.user_name for user in all_users]
    return all_unique_users
