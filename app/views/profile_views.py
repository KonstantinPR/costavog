from app import app
from flask import flash, render_template, request, redirect
from flask_login import login_required, current_user, login_user, logout_user
from app.models import Company, UserModel, db
from flask import url_for

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
            flash("Нет такой компании. Зарегистрируйте.")

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
            # return redirect('/transactions')
            return redirect('/profile')

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
        # user_name = request.form['user_name']
        password = request.form['password']

        company = Company(company_name=company_name)
        company.set_password(password)
        db.session.add(company)
        db.session.commit()

        # company_id = company.id
        # user = UserModel(user_name=user_name, company_id=company_id)
        # user.set_password(password)
        # db.session.add(user)
        # db.session.commit()

        # flash(f"Компания {company_name} с пользователем {user_name} зарегистрирована")
        flash(f"Компания {company_name} зарегистрирована")
        return render_template('user_register.html', company_name=company_name)

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
            flash("Нет такой компании или пароль не верен. Проверьте")
            return redirect('/user_register')

        if UserModel.query.filter_by(user_email=user_email).first():
            flash('Пользователь с таким email уже существует')
            return render_template('login.html', company_name=company_name, user_name=user_name)

        if UserModel.query.filter_by(company_id=company.id).count() > 0:
            # There are users in the database
            print(f"Users exist {UserModel.query.filter_by(company_id=company.id).count()}")
            user_role = app.config['USER_ROLE']
        else:
            user_role = app.config['ADMINISTRATOR_ROLE']
            # There are no users in the database
            print(f"No users exist, user_role {user_role}")

        user = UserModel(user_name=user_name, company_id=company_id, password_hash=new_user_password_hash,
                         user_email=user_email, role=user_role)

        db.session.add(user)
        db.session.commit()
        flash(f"Пользователь {user_name} зарегистрирован в компании {company_name} с правами {user_role}. ")
        return render_template('profile.html',
                               company_name=company_name,
                               user_name=user_name,
                               current_role=user_role,
                               roles=app.config['ROLES'])

    return render_template('user_register.html')


@app.route('/logout')
def logout():
    company_name = "Название компании"
    user_name = "Имя пользователя"
    if current_user.is_authenticated:
        company = Company.query.filter_by(id=current_user.company_id).first()
        company_name = company.company_name
        user_name = current_user.user_name
    logout_user()
    return render_template('login.html', company_name=company_name, user_name=user_name)


@app.route('/profile', methods=['POST', 'GET'])
@login_required
def profile():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    user_name = current_user.user_name

    if request.method == 'POST':
        # role = request.form.get('roles')
        role = app.config['USER_ROLE']
        if request.form.get('roles'): role = request.form.get('roles')
        initial_sum = request.form['initial_sum']
        if initial_sum == '': initial_sum = 0
        initial_file_path = request.form['initial_file_path']
        yandex_disk_token = request.form['yandex_disk_token']
        wb_api_token = request.form['wb_api_token']
        wb_api_token2 = request.form['wb_api_token2']

        user = UserModel.query.filter_by(id=current_user.id).first()
        user.initial_sum = initial_sum
        user.initial_file_path = initial_file_path
        # user.yandex_disk_token = yandex_disk_token
        user.role = role

        company = Company.query.filter_by(id=current_user.company_id).first()

        company.yandex_disk_token = yandex_disk_token
        company.wb_api_token = wb_api_token
        company.wb_api_token2 = wb_api_token2

        app.config['CURRENT_COMPANY_ID'] = company.id
        app.config['YANDEX_TOKEN'] = company.yandex_disk_token
        app.config['WB_API_TOKEN'] = company.wb_api_token
        app.config['WB_API_TOKEN2'] = company.wb_api_token2

        db.session.add(company)
        db.session.commit()

        flash("Изменения внесены")

        return render_template('profile.html',
                               user_name=user_name,
                               initial_sum=user.initial_sum,
                               initial_file_path=user.initial_file_path,
                               yandex_disk_token=company.yandex_disk_token,
                               wb_api_token=company.wb_api_token,
                               wb_api_token2=company.wb_api_token2,
                               current_role=role,
                               roles=app.config['ROLES']
                               )

    app.config['CURRENT_COMPANY_ID'] = Company.query.filter_by(id=current_user.company_id).one().id
    app.config['YANDEX_TOKEN'] = Company.query.filter_by(id=current_user.company_id).one().yandex_disk_token
    app.config['WB_API_TOKEN'] = Company.query.filter_by(id=current_user.company_id).one().wb_api_token
    app.config['WB_API_TOKEN2'] = Company.query.filter_by(id=current_user.company_id).one().wb_api_token2

    print(f"current_user.id {current_user.id}")
    print(f"current_company.id {app.config['CURRENT_COMPANY_ID']}")
    current_role = current_user.role
    roles = app.config['ROLES']
    administrator = app.config['ADMINISTRATOR_ROLE']
    initial_sum = current_user.initial_sum
    initial_file_path = current_user.initial_file_path
    yandex_disk_token = app.config['YANDEX_TOKEN']
    wb_api_token = app.config['WB_API_TOKEN']
    wb_api_token2 = app.config['WB_API_TOKEN2']
    points = current_user.points

    return render_template('profile.html',
                           user_name=user_name,
                           roles=roles,
                           current_role=current_role,
                           administrator=administrator,
                           points=points,
                           initial_sum=initial_sum,
                           initial_file_path=initial_file_path,
                           yandex_disk_token=yandex_disk_token,
                           wb_api_token=wb_api_token,
                           wb_api_token2=wb_api_token2,
                           )
