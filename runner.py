from app import app, db
from app.models import UserModel, Company, Task, Transaction, Product, login
import time


@app.shell_context_processor
def make_shell_context():
    return dict(app=app, db=db, User=UserModel, Company=Company, Task=Task, Transaction=Transaction,
                Product=Product)


@login.user_loader
def load_user(id):
    retry_count = 2  # Number of retry attempts
    retry_delay = 0.1  # Delay in seconds between retries

    for _ in range(retry_count):
        print(f'UserModel.query.get(int(id)) {UserModel.query.get(int(id))}')
        user = UserModel.query.get(int(id))
        if user is not None:
            return user
        else:
            # If user is None, wait for retry_delay seconds before trying again
            time.sleep(retry_delay)

    # If all retries fail, return None or raise an exception, depending on your requirements
    return UserModel.query.get(int(app.config['DEFAULT_ID']))  # Or raise an exception if needed


# @login.user_loader
# def load_user(id):
#     print(f'UserModel.query.get(int(id)) {UserModel.query.get(int(id))}')
#     return UserModel.query.get(int(id))


if __name__ == '__main__':
    app.run(host="localhost", port=8001, debug=True)
