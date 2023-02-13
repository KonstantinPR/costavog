from app import app
from flask import flash
from flask_login import current_user
from app.models import UserModel, Task, db, Company
import datetime
from sqlalchemy import desc
import yadisk
from random import randrange
import os
import shutil
import requests


def task_adding_in_db(request, company_id):
    description = request.form['description']

    if request.form['description'] == "":
        flash("Вы не ввели описание задачи, задача не добавлена")
        return None

    if request.form['date'] == "":
        date = datetime.date.today()
    else:
        date = request.form['date']

    if request.form['amount'] == "":
        amount = 1
    else:
        amount = request.form['amount']

    if request.form['all_users'] == '':
        executor_id = None
    else:
        executor_id = UserModel.query.filter_by(company_id=app.config["CURRENT_COMPANY_ID"],
                                                user_name=current_user.user_name).one().id

    print(request.form['all_users'])
    print(executor_id)

    user_name = current_user.user_name

    task = Task(amount=amount, description=description, date=date, user_name=user_name,
                company_id=company_id, executor_id=executor_id)
    db.session.add(task)
    db.session.commit()

    flash('Задача проведена')

    return task.id


def task_adding_yandex_disk(uploaded_files, added_task_id):
    print("uploaded_file" + str(uploaded_files))

    task = Task.query.filter_by(id=added_task_id).one()
    yandex_disk_token = Company.query.filter_by(id=current_user.company_id).one().yandex_disk_token
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
               'Authorization': f'OAuth {yandex_disk_token}'}
    y = yadisk.YaDisk(token=yandex_disk_token)
    yandex_disk_folder = 'TASKER/TASKS'
    if not y.exists(yandex_disk_folder):
        y.mkdir(yandex_disk_folder)
    task_directory = f"{str(task.id)}_{str(task.date)}_{str(task.user_name)}_" \
                     f"{str(task.description)[:20]}..."
    yandex_task_folder_path = f"{yandex_disk_folder}/{task_directory}"
    if not y.exists(yandex_task_folder_path):
        y.mkdir(yandex_task_folder_path)

    print('before flask request files if is folder task_directory  ' + str(task_directory))

    id_folder = randrange(1000000000000)
    tmp_folder = 'tmp_folder'
    files_folder = f"{tmp_folder}_{id_folder}"
    # для публичной ссылки на скачивание
    yandex_link = str
    if not os.path.exists(files_folder):
        os.makedirs(files_folder)
        for file in uploaded_files:
            file_path = f"{files_folder}/{file.filename}"
            file.save(file_path)
            yandex_task_file_path = f"{yandex_task_folder_path}/{file.filename}"
            y.upload(file_path, yandex_task_file_path)

        # путь на яндекс.диске к файлу (заносим в базу)
        yandex_link = yandex_task_folder_path
        task.yandex_link = yandex_link
        db.session.commit()

        # deleting yandex_disk_folder with images that was zipped
        try:
            shutil.rmtree(files_folder)
        except OSError as e:
            print("Error: %s - %s." % (e.filename, e.strerror))

        is_task_added_to_yandex_disk = f'Файлы были сохранены на Яндекс.Диск в каталог  {yandex_task_folder_path}'

    else:
        is_task_added_to_yandex_disk = f'Не сохранено на Яндекс.Диске. Вы не выбрали файлы, или они имеют недопустимый формат'

    return is_task_added_to_yandex_disk, yandex_link


def get_all_tasks_user(company_id):
    try:
        current_user_id = current_user.id
        if current_user.role == app.config['ADMINISTRATOR_ROLE']:
            tasks = db.session.query(Task).filter_by(company_id=company_id).order_by(
                desc(Task.condition), desc(Task.date), desc(Task.id)).all()
        else:
            tasks = db.session.query(Task).filter_by(company_id=company_id).filter(
                (Task.executor_id == None) | (Task.executor_id == current_user_id)).order_by(
                desc(Task.condition), desc(Task.date), desc(Task.id)).all()

    except ValueError:
        tasks = ""
        'Что-то не так с получением задач из базы данных'

    return tasks


def download_yandex_disk_tasks(id):
    task = Task.query.filter_by(id=id).one()
    yandex_disk_token = Company.query.filter_by(id=current_user.company_id).one().yandex_disk_token
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
               'Authorization': f'OAuth {yandex_disk_token}'}
    y = yadisk.YaDisk(token=yandex_disk_token)
    if task.yandex_link:
        task_yandex_disk_link = True
        return task_yandex_disk_link
        # if y.exists(task.yandex_link):
        #     task_yandex_disk_link = y.get_download_link(task.yandex_link)
        #     return task_yandex_disk_link

    task_yandex_disk_link = ""
    task.yandex_link = ""
    db.session.commit()

    return task_yandex_disk_link


def get_tasks_files(task_id):
    yandex_disk_token = Company.query.filter_by(id=current_user.company_id).one().yandex_disk_token
    y = yadisk.YaDisk(token=yandex_disk_token)
    task = Task.query.filter_by(id=task_id).first()
    files = y.listdir(task.yandex_link)
    images = []
    id_folder = randrange(1000000000000)
    static_path = 'app/static/'
    tmp_img = f'tmp_folder/tmp_img_{id_folder}/'
    tmp_folder = f'{static_path}{tmp_img}'
    files_folder = f"{tmp_folder}"
    if not os.path.exists(files_folder):
        os.makedirs(files_folder)
    for file in files:
        img = file.file
        string_start = '&filename='
        string_end = '.jpg'
        name_start = img.find(string_start)
        print(name_start)
        name_end = img.lower().find('.jpg')
        print(name_end)
        name_img = img[name_start + len(string_start):name_end + len(string_end)]
        print(name_img)
        url = img

        print(f"{tmp_folder}{name_img}")
        with requests.get(url, stream=True) as r:
            with open(f"{files_folder}{name_img}", "wb") as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)

        images.append(f"{tmp_img}{name_img}")

    return images
