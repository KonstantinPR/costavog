from app import app
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required, current_user, login_user, logout_user
from app.models import Company, UserModel, Transaction, Task, Product, db
import datetime
from sqlalchemy import desc
import flask
import yadisk
from app.modules import task_worker
from app import app
from flask import flash, render_template, request, redirect, send_file
import flask
from flask_login import login_required, current_user, login_user, logout_user
from app.models import Company, UserModel, Transaction, Task, Product, db
import datetime
from sqlalchemy import desc, asc, cast, Integer
import pandas as pd
from io import BytesIO
import numpy as np
import re
import yadisk
from random import randrange
from flask import url_for
import os
import shutil
from app.modules import transaction_worker
import urllib.request
import requests

# /// TASKS //////////////////

URL = app.config['URL']


@app.route('/tasks', methods=['POST', 'GET'])
@login_required
def tasks():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    company_id = current_user.company_id
    user_name = current_user.user_name

    if request.method == 'POST':
        # adding task in db
        task_id = task_worker.task_adding_in_db(request, company_id)

        # create tasks folder in yandex disk
        is_create_task_yandex_disk = request.form.getlist('is_create_task_yandex_disk')

        if is_create_task_yandex_disk:
            uploaded_files = flask.request.files.getlist("files")
            print(uploaded_files)
            is_adding_correct_msg, yandex_link = task_worker.task_adding_yandex_disk(uploaded_files, task_id)

            flash(is_adding_correct_msg)

    # вывод всех текущих операций под формой
    tasks = task_worker.get_all_tasks_user(company_id)

    return render_template('tasks.html', tasks=tasks)


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
        flash("Изменения внесены")


    else:
        task = Task.query.filter_by(id=id).first()
        amount = task.amount
        description = task.description
        date = task.date
        user_name = task.user_name
        users = db.session.query(UserModel).filter_by(company_id=company_id).all()
        executor_id = task.executor_id
        executor_name = None
        if executor_id:
            executor_user = db.session.query(UserModel).filter_by(id=executor_id).one()
            executor_name = executor_user.user_name
        user_name_set = set()
        for user in users:
            user_name_set.add(user.user_name)
        user_name_set.add('')
        task_yandex_disk_link = task_worker.download_yandex_disk_tasks(task.id)
        print(user_name_set)

        return render_template('task.html',
                               amount=amount,
                               description=description,
                               date=date,
                               user_name=user_name,
                               id=id,
                               condition=task.condition,
                               user_name_set=user_name_set,
                               executor_name=executor_name,
                               task_yandex_disk_link=task_yandex_disk_link
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
    task.condition = 'progress'
    db.session.add(task)
    db.session.commit()

    return redirect('/tasks')


@app.route('/task_complete/<int:id>', methods=['POST', 'GET'])
@login_required
def task_complete(id):
    if not current_user.is_authenticated:
        return redirect('/company_register')
    task = Task.query.filter_by(id=id).one()
    user_task_complete = db.session.query(UserModel).filter_by(id=task.executor_id).one()
    user_task_complete_points = user_task_complete.points
    if user_task_complete_points == None: user_task_complete_points = 0
    user = UserModel.query.filter_by(id=user_task_complete.id).one()
    user.points = int(user_task_complete_points) + int(task.amount)
    db.session.add(user)
    task = Task.query.filter_by(id=id).one()
    task.condition = 'completed'
    db.session.add(task)
    db.session.commit()
    flash(f'Задача {task.id} закрыта. Баланс {user_task_complete.user_name} {user.points} пнт.')

    return redirect('/tasks')


@app.route('/task_recover/<int:id>', methods=['POST', 'GET'])
@login_required
def task_recover(id):
    if not current_user.is_authenticated:
        return redirect('/company_register')
    task = Task.query.filter_by(id=id).one()
    user_task_complete = db.session.query(UserModel).filter_by(id=task.executor_id).one()
    user_task_complete_points = user_task_complete.points
    if user_task_complete_points == None: user_task_complete_points = 0
    user = UserModel.query.filter_by(id=user_task_complete.id).one()
    user.points = int(user_task_complete_points) - int(task.amount)
    db.session.add(user)
    task = Task.query.filter_by(id=id).one()
    task.condition = 'progress'
    db.session.add(task)
    db.session.commit()
    flash(f'Задача {task.id} восстановлена. Баланс {user_task_complete.user_name} {user.points} пнт.')

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


@app.route('/tasks_search', methods=['POST', 'GET'])
@login_required
def tasks_search():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    company_id = current_user.company_id

    if request.method == 'POST':
        search = request.form['search']
        tasks = db.session.query(Task).filter(Task.description.ilike('%' + search.lower() + '%')).order_by(
            desc(Task.date), desc(Task.id)).all()
        return render_template('tasks_div.html', tasks=tasks)

    return redirect('/tasks')


@app.route('/show_task_by_condition', methods=['POST', 'GET'])
@login_required
def show_task_by_condition():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    company_id = current_user.company_id

    if request.method == 'POST':
        task_condition = request.form['task_type_condition']
        tasks = db.session.query(Task).filter_by(condition=task_condition)
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
