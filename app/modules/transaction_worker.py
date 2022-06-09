from app import app
from flask import flash, render_template, request, redirect, send_file
import flask
from flask_login import login_required, current_user, login_user, logout_user
from app.models import Company, UserModel, Transaction, Task, Product, db
import datetime
from sqlalchemy import desc
import pandas as pd
from io import BytesIO
import numpy as np
import yadisk
from random import randrange
from flask import url_for
import os
import shutil
import requests


def transaction_adding_in_db(request, company_id):
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

    flash('Транзакция проведена')

    return transaction.id


def transaction_adding_yandex_disk(uploaded_files, added_transaction_id):
    print("uploaded_file" + str(uploaded_files))
    if any(uploaded_files):
        print(f"{uploaded_files} is true")

        transaction = Transaction.query.filter_by(id=added_transaction_id).one()
        yandex_disk_token = current_user.yandex_disk_token
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                   'Authorization': f'OAuth {yandex_disk_token}'}
        y = yadisk.YaDisk(token=yandex_disk_token)
        yandex_disk_folder = 'TASKER/FINANCE'
        transaction_directory = f"{str(transaction.id)}_{str(transaction.date)}_{str(transaction.user_name)}_" \
                                f"{str(transaction.description)[:20]}..."
        yandex_transaction_folder_path = f"{yandex_disk_folder}/{transaction_directory}"
        if not y.exists(yandex_transaction_folder_path):
            y.mkdir(yandex_transaction_folder_path)

        print('before flask request files if is folder transaction_directory  ' + str(transaction_directory))

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
                yandex_transaction_file_path = f"{yandex_transaction_folder_path}/{file.filename}"
                y.upload(file_path, yandex_transaction_file_path)

            # путь на яндекс.диске к файлу (заносим в базу)
            yandex_link = yandex_transaction_folder_path
            transaction.yandex_link = yandex_link
            db.session.commit()

            # deleting yandex_disk_folder with images that was zipped
            try:
                shutil.rmtree(files_folder)
            except OSError as e:
                print("Error: %s - %s." % (e.filename, e.strerror))

            is_transaction_added_to_yandex_disk = f'Файлы были сохранены на Яндекс.Диск в каталог  {yandex_transaction_folder_path}'
        else:
            is_transaction_added_to_yandex_disk = f'Возникли проблемы с сохранением файлов на Яндекс Диск. Файлы не сохранены'
    else:
        is_transaction_added_to_yandex_disk = f'Не сохранено на Яндекс.Диске. Вы не выбрали файлы, или они имеют недопустимый формат'

    return is_transaction_added_to_yandex_disk, yandex_link


def get_all_transactions_user(company_id):
    try:
        transactions = db.session.query(Transaction).filter_by(company_id=company_id).order_by(
            desc(Transaction.date), desc(Transaction.id)).all()
        users = UserModel.query.filter_by(id=current_user.id).first()
        initial_sum = users.initial_sum
        if not initial_sum:
            initial_sum = 0
        transactions_sum = initial_sum
        for i in transactions:
            if i.amount:
                transactions_sum += int(i.amount)
    except ValueError:
        transactions = ""
        transactions_sum = ""
        'something wrong in transactions'

    return transactions, transactions_sum


def download_yandex_disk_transactions(id):
    transaction = Transaction.query.filter_by(id=id).one()
    yandex_disk_token = current_user.yandex_disk_token
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
               'Authorization': f'OAuth {yandex_disk_token}'}
    y = yadisk.YaDisk(token=yandex_disk_token)
    if transaction.yandex_link:
        if y.exists(transaction.yandex_link):
            transaction_yandex_disk_link = y.get_download_link(transaction.yandex_link)
            return transaction_yandex_disk_link

    transaction_yandex_disk_link = ""
    transaction.yandex_link = ""
    db.session.commit()

    return transaction_yandex_disk_link


def get_transactions_files(transaction_id):
    yandex_disk_token = current_user.yandex_disk_token
    y = yadisk.YaDisk(token=yandex_disk_token)
    transaction = Transaction.query.filter_by(id=transaction_id).first()
    files = y.listdir(transaction.yandex_link)
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
