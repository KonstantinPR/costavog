from flask import Flask, flash, render_template, request, redirect
from flask_migrate import Migrate
from flask_login import login_required, current_user, login_user, logout_user
from models import Company, UserModel, Post, Task, db, login
import datetime
from werkzeug.security import generate_password_hash, check_password_hash
import os
from os import environ

app = Flask(__name__)
migrate = Migrate(app, db)
app.secret_key = 'xyz'

app.config['SQLALCHEMY_DATABASE_URI'] = environ.get('DATABASE_URL') or 'postgresql://postgres:19862814@localhost/data'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

#  to solve problems connection with SQLAlchemy > 1.4
uri_old = os.getenv("DATABASE_URL")  # or other relevant config var
uri = environ.get('DATABASE_URL')
if uri:
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)

ura = 'Ura Gagarin'
print(ura)
if ura:
    print(ura)
    if ura.startswith("Ura"):
        ura = ura.replace("Gagarin", "Gagarka", 1)

db.init_app(app)

# with app.app_context():
#     db.create_all()

login.init_app(app)
login.login_view = 'login'


@app.route('/hello')
def hello():
    database_uri = environ.get('DATABASE_URL')
    return ("hello this is database_uri= " + str(database_uri) + ' and Ura = ' + ura + ' uri_old = ' + uri_old)


# @app.before_first_request
@app.route('/create-all')
def create_all():
    db.create_all()


# ///POSTS////////////


# @app.route('/', methods=['POST', 'GET'])
# @login_required
# def index():
#     if not current_user.is_authenticated:
#         return "hello unregister friend"
#     else:
#         return "hello registered friend"


@app.route('/blog', methods=['POST', 'GET'])
@app.route('/', methods=['POST', 'GET'])
@login_required
def blog():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    company_id = current_user.company_id
    if request.method == 'POST':
        amount = request.form['amount']
        description = request.form['description']

        if request.form['date'] == "":
            date = datetime.date.today()
        else:
            date = request.form['date']
        if request.form['user_name'] == "":
            user_name = current_user.user_name
        else:
            user_name = request.form['user_name']

        post = Post(amount=amount, description=description, date=date, user_name=user_name, company_id=company_id)
        db.session.add(post)
        db.session.commit()

    user_name = current_user.user_name
    try:
        posts = db.session.query(Post).filter_by(company_id=company_id).all()
    except ValueError:
        posts = ""
        'base is empty'
    return render_template('blog.html', posts=posts, user_name=user_name)


@app.route('/post_edit/<int:id>', methods=['POST', 'GET'])
def post_edit(id):
    if request.method == 'POST':
        amount = request.form['amount']
        description = request.form['description']
        date = request.form['date']
        user_name = request.form['user_name']
        post = Post.query.filter_by(id=id).one()
        post.amount = amount
        post.description = description
        post.date = date
        post.user_name = user_name
        db.session.add(post)
        db.session.commit()
        flash("Changing completed")

    else:
        post = Post.query.filter_by(id=id).first()
        amount = post.amount
        description = post.description
        date = post.date
        user_name = post.user_name
        return render_template('post.html',
                               amount=amount,
                               description=description,
                               date=date,
                               user_name=user_name,
                               id=id)

    return redirect('/blog')


@app.route('/post_delete/<int:id>', methods=['POST', 'GET'])
def post_delete(id):
    flash("Запись удалена")
    post = Post.query.filter_by(id=id).one()
    db.session.delete(post)
    db.session.commit()

    return render_template('blog.html')


# ///TASKS//////////////////


@app.route('/tasks', methods=['POST', 'GET'])
@login_required
def tasks():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    company_id = current_user.company_id
    print(company_id)
    if request.method == 'POST':
        description = request.form['description']

        if request.form['date'] == "":
            date = datetime.date.today()
        else:
            date = request.form['date']
        if request.form['amount'] == "":
            amount = 1
        else:
            amount = request.form['amount']
        if request.form['user_name'] == "":
            user_name = current_user.user_name
        else:
            user_name = request.form['user_name']

        task = Task(amount=amount, description=description, date=date, user_name=user_name, company_id=company_id)
        db.session.add(task)
        db.session.commit()

    user_name = current_user.user_name
    tasks = db.session.query(Task).filter_by(company_id=company_id).all()
    return render_template('tasks.html', tasks=tasks, user_name=user_name)


@app.route('/task_edit/<int:id>', methods=['POST', 'GET'])
def task_edit(id):
    if not current_user.is_authenticated:
        return redirect('/company_register')
    company_id = current_user.company_id

    if request.method == 'POST':
        amount = request.form['amount']
        description = request.form['description']
        date = request.form['date']
        user_name = request.form['user_name']
        task = Task.query.filter_by(id=id).one()
        task.amount = amount
        task.description = description
        task.date = date
        task.user_name = user_name
        db.session.add(task)
        db.session.commit()
        flash("Changing completed")

    else:
        task = Task.query.filter_by(id=id).first()
        amount = task.amount
        description = task.description
        date = task.date
        user_name = task.user_name
        return render_template('task.html',
                               amount=amount,
                               description=description,
                               date=date,
                               user_name=user_name,
                               id=id)

    tasks = db.session.query(Task).filter_by(company_id=company_id).all()
    return redirect('/tasks')


@app.route('/task_delete/<int:id>', methods=['POST', 'GET'])
def task_delete(id):
    flash("Запись удалена")
    task = Task.query.filter_by(id=id).one()
    db.session.delete(task)
    db.session.commit()

    return render_template('tasks.html')


@app.route('/login', methods=['POST', 'GET'])
def login():
    if request.method == 'POST':
        company_name = request.form['company_name']
        user_name = request.form['user_name']
        password = request.form['password']
        remember = True if request.form.get('remember') else False
        print(remember)

        company_id = Company.query.filter_by(company_name=company_name).first()
        if not company_id:
            flash("No such company name registered")
            return render_template('login.html', company_name=request.form['company_name'])

        if check_password_hash(company_id.password_hash, password):
            user = UserModel.query.filter_by(user_name=user_name, company_id=company_id.id).first()

        if user is not None:
            login_user(user, remember=remember)
            return redirect('/blog')

    if current_user.is_authenticated:
        company = Company.query.filter_by(id=current_user.company_id).first()
        company_name = company.company_name
    else:
        company_name = ""

    return render_template('login.html', company_name=company_name)


@app.route('/company_register', methods=['POST', 'GET'])
def company_register():
    if request.method == 'POST':
        company_name = request.form['company_name']
        user_name = request.form['user_name']
        password = request.form['password']

        company = Company(company_name=company_name)
        company.set_password(password)
        db.session.add(company)
        db.session.commit()

        company_id = company.id
        user = UserModel(user_name=user_name, company_id=company_id)
        user.set_password(password)
        db.session.add(user)
        db.session.commit()

        db.session.commit()
        flash(f"Компания {company_name} с пользователем {user_name} зарегистрирована")
        return render_template('login.html', company_name=company_name, user_name=user_name)

    return render_template('company_register.html')


@app.route('/user_register', methods=['POST', 'GET'])
def user_register():
    if request.method == 'POST':

        company_name = request.form['company_name']
        password = request.form['password']
        password_hash = generate_password_hash(password)
        print(password_hash)
        user_name = request.form['user_name']

        company = Company.query.filter_by(company_name=company_name).first()
        if check_password_hash(company.password_hash, password):
            company_id = company.id
        else:
            print("No such company")

        if UserModel.query.filter_by(user_name=user_name).first():
            return ('User_name already Present')

        user = UserModel(user_name=user_name, company_id=company_id)
        db.session.add(user)
        db.session.commit()
        return redirect('/blog')

    return render_template('user_register.html')


@app.route('/logout')
def logout():
    logout_user()
    return redirect('/blog')


if __name__ == '__main__':
    app.run(host="localhost", port=8001, debug=True)
