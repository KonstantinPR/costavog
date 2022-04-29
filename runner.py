from app import app, db
from app.models import UserModel, Company, Task, Transaction, Product


@app.shell_context_processor
def make_shell_context():
    return dict(app=app, db=db, User=UserModel, Company=Company, Task=Task, Transaction=Transaction,
                Product=Product)


if __name__ == '__main__':
    app.run(host="localhost", port=8001, debug=True)
