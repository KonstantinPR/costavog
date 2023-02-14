from app import app, db
from app.models import UserModel, Company, Task, Transaction, Product, login


@app.shell_context_processor
def make_shell_context():
    return dict(app=app, db=db, User=UserModel, Company=Company, Task=Task, Transaction=Transaction,
                Product=Product)


@login.user_loader
def load_user(id):
    return UserModel.query.get(int(id))

# def set_config():
#     print(f"Setting config for current_user {current_user}")
#     if session.get('CURRENT_COMPANY_ID') == None or current_user:
#         session['CURRENT_COMPANY_ID'] = current_user.company_id
#         session['YANDEX_TOKEN'] = Company.query.filter_by(id=current_user.company_id).one().yandex_disk_token
#         session['WB_API_TOKEN'] = Company.query.filter_by(id=current_user.company_id).one().wb_api_token
#         session['WB_API_TOKEN2'] = Company.query.filter_by(id=current_user.company_id).one().wb_api_token2
#         app.config['CURRENT_COMPANY_ID'] = session['CURRENT_COMPANY_ID']
#         app.config['YANDEX_TOKEN'] = app.config['YANDEX_TOKEN']
#         app.config['WB_API_TOKEN'] = app.config['WB_API_TOKEN']
#         app.config['WB_API_TOKEN2'] = app.config['WB_API_TOKEN2']
#         print(f"For current_user {current_user} config is updated")
#     else:
#         return redirect("/login")
#     return None
#
#
# set_config()

if __name__ == '__main__':
    app.run(host="localhost", port=8001, debug=True)
