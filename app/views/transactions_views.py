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


# ///TRANSACTIONS////////////

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

        # create transactions folder in yandex disk
        is_create_transaction_yandex_disk = request.form.getlist('is_create_transaction_yandex_disk')
        if is_create_transaction_yandex_disk:

            uploaded_file = request.files['files']
            if uploaded_file.filename != '':

                yandex_disk_token = current_user.yandex_disk_token
                headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                           'Authorization': f'OAuth {yandex_disk_token}'}
                y = yadisk.YaDisk(token=yandex_disk_token)
                directory = 'TASKER/FINANCE'
                transaction_directory = str(transaction.id) + '_' + str(date) + '_' + str(
                    transaction.user_name) + '_' + str(
                    transaction.description)[:20] + "..."
                yandex_transaction_folder_path = f"{directory}/{transaction_directory}"
                if not y.exists(yandex_transaction_folder_path):
                    y.mkdir(yandex_transaction_folder_path)

                # if not y.exists(yandex_transaction_folder_path + '/' + transaction_directory):
                #     y.mkdir(yandex_transaction_folder_path + '/' + transaction_directory)
                print('before flask request files if is folder transaction_directory  ' + str(transaction_directory))

                id_folder = randrange(1000000000000)
                files_folder = f"tmp_folder_{id_folder}"
                if not os.path.exists(files_folder):
                    os.makedirs(files_folder)
                    file_path = f"{files_folder}/{uploaded_file.filename}"
                    uploaded_file.save(file_path)
                    yandex_transaction_file_path = f"{yandex_transaction_folder_path}/{uploaded_file.filename}"

                    y.upload(file_path, yandex_transaction_file_path)

                    # deleting directory with images that was zipped
                    try:
                        shutil.rmtree(files_folder)
                    except OSError as e:
                        print("Error: %s - %s." % (e.filename, e.strerror))

                    flash(f'Файлы были сохранены на Яндекс.Диск в каталог  {yandex_transaction_file_path}')

    user_name = current_user.user_name

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
        'something wrong in transactions and tranactions'

    print('vot oni ' + str(transactions))

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
@login_required
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


@app.route('/transaction_copy', methods=['POST', 'GET'])
@login_required
def transaction_copy():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    company_id = current_user.company_id

    if request.method == 'POST':
        amount = request.form['amount']
        description = request.form['description']
        date = datetime.date.today()
        user_name = request.form['user_name']
        transaction = Transaction(amount=amount, description=description, date=date, user_name=user_name,
                                  company_id=company_id)
        db.session.add(transaction)
        db.session.commit()
        flash("Транзакция скопирована")

    return redirect('/transactions')


@app.route('/transaction_search', methods=['POST', 'GET'])
@login_required
def transaction_search():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    company_id = current_user.company_id

    search = request.form['search']
    transactions = db.session.query(Transaction).filter(
        Transaction.description.ilike('%' + search.lower() + '%'), Transaction.company_id == company_id).order_by(
        desc(Transaction.date), desc(Transaction.id)).all()

    print(transactions)

    return render_template('transactions_div.html', transactions=transactions)


@app.route('/transaction_delete/<int:id>', methods=['POST', 'GET'])
@login_required
def transaction_delete(id):
    flash("Запись удалена")
    transaction = Transaction.query.filter_by(id=id).one()
    db.session.delete(transaction)
    db.session.commit()

    return redirect('/transactions')
