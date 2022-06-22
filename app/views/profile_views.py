from app import app
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required, current_user, login_user, logout_user
from app.models import Company, UserModel, Transaction, Task, Product, db

from werkzeug.security import generate_password_hash, check_password_hash


# /// PROFILE //////////////////

@app.route('/login', methods=['POST', 'GET'])
def login():
    if request.method == 'POST':
        company_name = request.form['company_name']
        user_name = request.form['user_name']
        password = request.form['password']
        remember = True if request.form.get('remember') else False
        print(remember)

        company = Company.query.filter_by(company_name=company_name).first()
        if not company:
            flash("Нет такой компании")
            return render_template('login.html', company_name=request.form['company_name'])

        user = UserModel.query.filter_by(user_name=user_name, company_id=company.id).first()

        if not user:
            flash("Пользователя с таким именем не найдено")
            return render_template('login.html', company_name=request.form['company_name'])

        if not check_password_hash(user.password_hash, password):
            flash("Неверно указан пароль")
            return render_template('login.html', company_name=request.form['company_name'])

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
        current_company_password = request.form['current_company_password']
        new_user_password = request.form['new_user_password']
        new_user_password_hash = generate_password_hash(new_user_password)
        user_name = request.form['user_name']
        user_email = request.form['user_email']

        company = Company.query.filter_by(company_name=company_name).first()
        if check_password_hash(company.password_hash, current_company_password):
            company_id = company.id
        else:
            print("Нет такой компании")

        if UserModel.query.filter_by(user_email=user_email).first():
            flash('Пользователь с таким email уже существует')
            return ('Пользователь с таким email уже существует')

        user = UserModel(user_name=user_name, company_id=company_id, password_hash=new_user_password_hash,
                         user_email=user_email)
        db.session.add(user)
        db.session.commit()
        return redirect('/transactions')

    return render_template('user_register.html')


@app.route('/logout')
def logout():
    logout_user()
    return redirect('/transactions')


@app.route('/profile', methods=['POST', 'GET'])
@login_required
def profile():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    user_name = current_user.user_name

    if request.method == 'POST':
        initial_sum = request.form['initial_sum']
        initial_file_path = request.form['initial_file_path']
        yandex_disk_token = request.form['yandex_disk_token']

        user = UserModel.query.filter_by(id=current_user.id).first()
        user.initial_sum = initial_sum
        user.initial_file_path = initial_file_path
        user.yandex_disk_token = yandex_disk_token
        db.session.commit()

        flash("Changing completed")

        return render_template('profile.html', user_name=user_name, initial_sum=initial_sum,
                               initial_file_path=initial_file_path,
                               yandex_disk_token=yandex_disk_token)

    initial_sum = current_user.initial_sum
    initial_file_path = current_user.initial_file_path
    yandex_disk_token = app.config['YANDEX_TOKEN']
    points = current_user.points

    return render_template('profile.html', user_name=user_name, initial_sum=initial_sum,
                           initial_file_path=initial_file_path,
                           yandex_disk_token=yandex_disk_token,
                           points=points)
