import flask
from flask import Flask, flash, render_template, request, redirect, send_file
from flask_migrate import Migrate
from flask_login import login_required, current_user, login_user, logout_user
from models import Company, UserModel, Transaction, Task, Product, db, login
import datetime
from werkzeug.security import generate_password_hash, check_password_hash
import os
from os import environ
from sqlalchemy import desc
import pandas as pd
from io import BytesIO
import io
import numpy as np
from sqlalchemy import create_engine
from modules import discount, detailing

app = Flask(__name__)
app.config.from_pyfile('config.py')
migrate = Migrate(app, db)
app.secret_key = 'xyz'

#  to solve problems connection with SQLAlchemy > 1.4 in heroku
uri_old = os.getenv("DATABASE_URL")  # or other relevant config var
uri = environ.get('DATABASE_URL')
if uri:
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = uri or 'postgresql+psycopg2://postgres:19862814@localhost/data'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)

# with app.app_context():
#     db.create_all()

login.init_app(app)
login.login_view = 'login'


@app.before_first_request
def create_all():
    db.create_all()


# ///PRODUCTS////////////


@app.route('/upload_products', methods=['POST', 'GET'])
@login_required
def upload_products():
    if not current_user.is_authenticated:
        return redirect('/company_register')

    if request.method == 'POST':
        uploaded_files = flask.request.files.getlist("file")
        df = pd.read_excel(uploaded_files[0])
        df.replace(np.NaN, "", inplace=True)
        df["id"] = df.index + 1
        col_list = ['id', 'Артикул поставщика БАЗА', 'Себестоимость БАЗА']
        df = df[col_list].rename(columns={'Артикул поставщика БАЗА': 'article', 'Себестоимость БАЗА': 'net_cost'})

        engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
        df.head(0).to_sql('products', engine, if_exists='replace',
                          index=False)  # drops old table and creates new empty table

        conn = engine.raw_connection()
        cur = conn.cursor()
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        contents = output.getvalue()
        cur.copy_from(output, 'products', null="")  # null values become ''
        conn.commit()
        flash("Изменения в базе произведены успешно")
        print(df)

    return render_template('upload_products.html')


def io_output(df):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer)
    writer.close()
    output.seek(0)
    return output

@app.route('/upload_turnover', methods=['POST', 'GET'])
@login_required
def upload_turnover():
    if not current_user.is_authenticated:
        return redirect('/company_register')

    if request.method == 'POST':
        uploaded_files = flask.request.files.getlist("file")
        df = pd.read_excel(uploaded_files[0])
        df.replace(np.NaN, "", inplace=True)
        df_products = pd.read_sql(db.session.query(Product).statement, db.session.bind)
        df = discount.discount(df, df_products)
        file = io_output(df)

        return send_file(file, attachment_filename="excel.xlsx", as_attachment=True)

    return render_template('upload_turnover.html')


@app.route('/upload_detailing', methods=['POST', 'GET'])
@login_required
def upload_detailing():
    if not current_user.is_authenticated:
        return redirect('/company_register')

    if request.method == 'POST':
        uploaded_files = flask.request.files.getlist("file")
        df = detailing.zip_detale(uploaded_files[0])
        file = io_output(df)

        return send_file(file, attachment_filename='report' + str(datetime.date.today()) + ".xlsx", as_attachment=True)

    return render_template('upload_detailing.html')


# ///POSTS////////////


# @app.route('/', methods=['POST', 'GET'])
# @login_required
# def index():
#     if not current_user.is_authenticated:
#         return "hello unregister friend"
#     else:
#         return "hello registered friend"


@app.route('/transactions', methods=['POST', 'GET'])
@app.route('/', methods=['POST', 'GET'])
@login_required
def transactions():
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

        user_name = current_user.user_name

        transaction = Transaction(amount=amount, description=description, date=date, user_name=user_name,
                                  company_id=company_id)
        db.session.add(transaction)
        db.session.commit()

    user_name = current_user.user_name
    try:
        transactions = db.session.query(Transaction).filter_by(company_id=company_id).order_by(
            desc(Transaction.date)).all()
        users = UserModel.query.filter_by(id=current_user.id).first()
        initial_sum = users.initial_sum
        if not initial_sum:
            initial_sum = 0
        transactions_sum = initial_sum

        for i in transactions:
            transactions_sum += int(i.amount)
    except ValueError:
        transactions = ""
        transactions_sum = ""
        'base is empty'
    return render_template('transactions.html', transactions=transactions, user_name=user_name,
                           transactions_sum=transactions_sum)


@app.route('/transactions_to_excel')
@login_required
def transactions_to_excel():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    company_id = current_user.company_id
    try:
        df = pd.read_sql(db.session.query(Transaction).filter_by(company_id=company_id).statement, db.session.bind)
    except ValueError:
        df = []

    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer)
    writer.close()
    output.seek(0)

    return send_file(output, attachment_filename="excel.xlsx", as_attachment=True)


def correct_sum(amount, account):
    if account == 801:
        amount = -amount
    amount = int(amount)
    return amount


def correct_desc(description, desc_income):
    if description == "":
        description = desc_income
    else:
        description = str(description) + " | " + str(desc_income)
    return description


@app.route('/upload_transaction_excel', methods=['POST', 'GET'])
@login_required
def upload_transaction_excel():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    company_id = current_user.company_id

    # if request.method == 'POST':
    #     uploaded_files = flask.request.files.getlist("file")
    #     df = pd.read_excel(uploaded_files[0])

    path = str(current_user.initial_file_path)
    df = pd.read_excel(path)
    df.replace(np.NaN, "", inplace=True)

    df['СУММА ПРИХОДА'] = [correct_sum(amount, account) for amount, account in
                           zip(df['СУММА ПРИХОДА'],
                               df['СЧЕТ РАСХОДА'],
                               )]

    df['ОПИСАНИЕ ОПЕРАЦИИ'] = [correct_desc(description, desc_income) for description, desc_income in
                               zip(df['ОПИСАНИЕ ОПЕРАЦИИ'],
                                   df['ОПИСАНИЕ ПРИХОДА'],
                                   )]

    len_data = len(df['СУММА ПРИХОДА'])
    sum = df['СУММА ПРИХОДА'].sum()
    print(sum)

    return render_template("transactions_excel.html",
                           data=[df["ДАТА"], df['СУММА ПРИХОДА'], df["ОПИСАНИЕ ОПЕРАЦИИ"]],
                           len_data=len_data, sum=sum)


@app.route('/transaction_edit/<int:id>', methods=['POST', 'GET'])
def transaction_edit(id):
    if request.method == 'POST':
        amount = request.form['amount']
        description = request.form['description']
        date = request.form['date']
        user_name = request.form['user_name']
        transaction = Transaction.query.filter_by(id=id).one()
        transaction.amount = amount
        transaction.description = description
        transaction.date = date
        transaction.user_name = user_name
        db.session.add(transaction)
        db.session.commit()
        flash("Changing completed")

    else:
        transaction = Transaction.query.filter_by(id=id).first()
        amount = transaction.amount
        description = transaction.description
        date = transaction.date
        user_name = transaction.user_name
        return render_template('transaction.html',
                               amount=amount,
                               description=description,
                               date=date,
                               user_name=user_name,
                               id=id)

    return redirect('/transactions')


@app.route('/transaction_delete/<int:id>', methods=['POST', 'GET'])
def transaction_delete(id):
    flash("Запись удалена")
    transaction = Transaction.query.filter_by(id=id).one()
    db.session.delete(transaction)
    db.session.commit()

    return redirect('/transactions')


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
        user_name = current_user.user_name

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

    return redirect('/tasks')


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
            return redirect('/transactions')

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
        return redirect('/transactions')

    return render_template('user_register.html')


@app.route('/logout')
def logout():
    logout_user()
    return redirect('/transactions')


@app.route('/pfofile', methods=['POST', 'GET'])
@login_required
def profile():
    if not current_user.is_authenticated:
        return redirect('/company_register')

    if request.method == 'POST':
        initial_sum = request.form['initial_sum']
        initial_file_path = request.form['initial_file_path']

        users = UserModel.query.filter_by(id=current_user.id).first()
        users.initial_sum = initial_sum
        users.initial_file_path = initial_file_path
        db.session.commit()

        flash("Changing completed")

    initial_sum = current_user.initial_sum
    initial_file_path = current_user.initial_file_path

    return render_template('profile.html', initial_sum=initial_sum, initial_file_path=initial_file_path)


if __name__ == '__main__':
    app.run(host="localhost", port=8001, debug=True)
