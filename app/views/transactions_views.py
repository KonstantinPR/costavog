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
from app.modules import transaction_worker


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
    user_name = current_user.user_name

    if request.method == 'POST':
        # adding transaction in db
        transaction_id = transaction_worker.transaction_adding_in_db(request, company_id)

        # create transactions folder in yandex disk
        is_create_transaction_yandex_disk = request.form.getlist('is_create_transaction_yandex_disk')

        if is_create_transaction_yandex_disk:
            uploaded_files = flask.request.files.getlist("files")
            is_adding_correct_msg, yandex_link = transaction_worker.transaction_adding_yandex_disk(uploaded_files,
                                                                                                   transaction_id)

            flash(is_adding_correct_msg)

    # вывод всех текущих операций под формой
    transactions, transactions_sum = transaction_worker.get_all_transactions_user(company_id)

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
        transaction_yandex_disk_link = transaction_worker.download_yandex_disk(transaction.id)

        return render_template('transaction.html',
                               transaction=transaction, transaction_yandex_disk_link=transaction_yandex_disk_link)

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
