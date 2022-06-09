from app import app
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required, current_user, login_user, logout_user
from app.models import Company, UserModel, Transaction, Task, Product, db
import datetime
from sqlalchemy import desc
import yadisk

# /// TASKS //////////////////

URL = app.config['URL']


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

        # create yandex disk catalog on that task if checkbox is on
        is_create_yandex_disk_catalog = request.form.getlist('is_create_yandex_disk_catalog')
        if is_create_yandex_disk_catalog:
            yandex_disk_token = current_user.yandex_disk_token
            headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                       'Authorization': f'OAuth {yandex_disk_token}'}
            y = yadisk.YaDisk(token=yandex_disk_token)
            directory = 'TASKER'
            task_directory = str(task.id) + '_' + str(date) + '_' + str(task.user_name) + '_' + str(
                task.description)[:20] + "..."
            if not y.exists(directory):
                y.mkdir(directory)

            if not y.exists(directory + '/' + task_directory):
                y.mkdir(directory + '/' + task_directory)

    user_name = current_user.user_name
    tasks = db.session.query(Task).filter_by(company_id=company_id).order_by(desc(Task.date), desc(Task.id)).all()

    return render_template('tasks.html', tasks=tasks, user_name=user_name)


@app.route('/task_edit/<int:id>', methods=['POST', 'GET'])
@login_required
def task_edit(id):
    if not current_user.is_authenticated:
        return redirect('/company_register')
    company_id = current_user.company_id

    if request.method == 'POST':
        amount = request.form['amount']
        description = request.form['description']
        date = request.form['date']
        user_name = request.form['user_name']
        executor_name = request.form['executor_name']
        executor_id = None
        if executor_name:
            executor_user = db.session.query(UserModel).filter_by(company_id=company_id, user_name=executor_name).one()
            executor_id = executor_user.id

        task = Task.query.filter_by(id=id).one()
        task.amount = amount
        task.description = description
        task.date = date
        task.user_name = user_name
        task.executor_id = executor_id
        db.session.add(task)
        db.session.commit()
        flash("Changing completed")


    else:
        task = Task.query.filter_by(id=id).first()
        executor_id = task.executor_id
        amount = task.amount
        description = task.description
        date = task.date
        user_name = task.user_name
        users = db.session.query(UserModel).filter_by(company_id=company_id).all()
        executor_id = None
        executor_name = None
        if executor_id:
            executor_user = db.session.query(UserModel).filter_by(id=executor_id).one()
            executor_name = executor_user.user_name
        user_name_set = set()
        for user in users:
            user_name_set.add(user.user_name)
        user_name_set.add('')
        print(user_name_set)

        return render_template('task.html',
                               amount=amount,
                               description=description,
                               date=date,
                               user_name=user_name,
                               id=id,
                               user_name_set=user_name_set,
                               executor_name=executor_name
                               )

    # tasks = db.session.query(Task).filter_by(company_id=company_id).all()
    return redirect('/tasks')


@app.route('/task_take_work/<int:id>', methods=['POST', 'GET'])
@login_required
def task_take_work(id):
    if not current_user.is_authenticated:
        return redirect('/company_register')

    task = Task.query.filter_by(id=id).one()
    task.executor_id = current_user.id
    db.session.add(task)
    db.session.commit()

    return redirect('/tasks')


@app.route('/task_copy', methods=['POST', 'GET'])
@login_required
def task_copy():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    company_id = current_user.company_id

    if request.method == 'POST':
        amount = request.form['amount']
        description = request.form['description']
        date = datetime.date.today()
        user_name = request.form['user_name']
        task = Task(amount=amount, description=description, date=date, user_name=user_name, company_id=company_id)
        db.session.add(task)
        db.session.commit()

        flash("Changing completed")

    return redirect('/tasks')


@app.route('/task_search', methods=['POST', 'GET'])
@login_required
def task_search():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    company_id = current_user.company_id

    if request.method == 'POST':
        search = request.form['search']
        tasks = db.session.query(Task).filter(Task.description.ilike('%' + search.lower() + '%')).order_by(
            desc(Task.date), desc(Task.id)).all()
        return render_template('tasks.html', tasks=tasks)

    return redirect('/tasks')


@app.route('/task_delete/<int:id>', methods=['POST', 'GET'])
@login_required
def task_delete(id):
    flash("Запись удалена")
    task = Task.query.filter_by(id=id).one()
    db.session.delete(task)
    db.session.commit()

    return redirect('/tasks')
