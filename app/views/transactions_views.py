import logging
from app import app
from flask import flash, render_template, request, redirect, send_file
import flask
from flask_login import login_required, current_user
from app.models import Transaction, db
import datetime
import pandas as pd
from io import BytesIO
import numpy as np
from app.modules import transaction_worker, decorators


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
# @app.route('/', methods=['POST', 'GET'])
@login_required
@decorators.administrator_required
def transactions():
    """
    Учет расходов. Данные сохраняются в базе. Баланс - текущее кол-во денег, если отрицательное число - долг фирмы.
    Сумма - в рублях. Дата - когда была совершена транзакция. Можно выбрать и прикрепить изображение (скриншот)
    документа или чека, а также любой другой картинки. Чекбокс "не учитывать в транзакциях" значит, что эта сумма
    не будет влиять на Баланс, но будет присутствовать в списке и базе транзакций (т.е. вносится для личных целей).
    Есть поиск, в котором можно фильтровать транзакции по части строки, ищет вхождения - в описании. Также если в начале
    добавить слово all а затем через пробел то что ищем - то будут показаны все транзакции, включая не учитывающиеся.

    """
    if not current_user.is_authenticated:
        return redirect('/login')
    company_id = current_user.company_id
    user_name = current_user.user_name

    if request.method == 'POST':
        # adding transaction in db
        transaction_id = transaction_worker.transaction_adding_in_db(request, company_id)

        # create transactions folder in YandexDisk
        is_create_transaction_YandexDisk = request.form.getlist('is_create_transaction_YandexDisk')
        uploaded_files = flask.request.files.getlist("files")
        print(f"upload files {uploaded_files}")

        if any(uploaded_files):
            is_adding_correct_msg, yandex_link = transaction_worker.transaction_adding_YandexDisk(uploaded_files,
                                                                                                   transaction_id)

            flash(is_adding_correct_msg)

    # вывод всех текущих операций под формой
    transactions, transactions_sum = transaction_worker.get_transactions(company_id)

    return render_template('transactions.html', transactions=transactions, user_name=user_name,
                           transactions_sum=transactions_sum, sort_type='asc', sort_sign='', )


@app.route('/transactions_by/<field_type>/<sort_type>', methods=['POST', 'GET'])
@login_required
def transactions_by(field_type, sort_type):
    if not current_user.is_authenticated:
        return redirect('/login')
    company_id = current_user.company_id
    user_name = current_user.user_name
    # все текущие операции отсортированные по field_type (дата, описание ...)

    field_type = field_type
    if sort_type == 'desc':
        sort_type = 'asc'
        sort_sign = '&#9660;'

        sql_preparing = f"db.session.query(Transaction).filter_by(company_id=company_id)." \
                        f"order_by(desc(Transaction.{field_type}),desc(Transaction.id)).all()"

        if field_type == 'amount':
            sql_preparing = f"db.session.query(Transaction).filter_by(company_id=company_id)." \
                            f"order_by(desc(cast(Transaction.{field_type},Integer)),desc(Transaction.id)).all()"
    else:
        sort_type = 'desc'
        sort_sign = '&#9650;'

        sql_preparing = f"db.session.query(Transaction).filter_by(company_id=company_id)." \
                        f"order_by(asc(Transaction.{field_type}),desc(Transaction.id)).all()"

        if field_type == 'amount':
            sql_preparing = f"db.session.query(Transaction).filter_by(company_id=company_id)." \
                            f"order_by(asc(cast(Transaction.{field_type},Integer)),desc(Transaction.id)).all()"

    transactions = eval(sql_preparing)

    return render_template('transactions_div.html',
                           transactions=transactions,
                           field_type=field_type,
                           sort_type=sort_type,
                           sort_sign=sort_sign)


@app.route('/transactions_to_excel')
@login_required
def transactions_to_excel():
    if not current_user.is_authenticated:
        return redirect('/login')
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

    return send_file(output, download_name="excel.xlsx", as_attachment=True)


@app.route('/upload_transaction_excel', methods=['POST', 'GET'])
@login_required
def upload_transaction_excel():
    if not current_user.is_authenticated:
        return redirect('/login')
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
    if not current_user.is_authenticated:
        return redirect('/login')
    if request.method == 'POST':
        transaction = Transaction.query.filter_by(id=id).one()
        amount = request.form['amount']
        description = request.form['description']
        date = request.form['date']
        user_name = request.form['user_name']
        is_private = 0
        if request.form.get('is_private'):
            is_private = request.form.get('is_private')

        transaction.amount = amount
        transaction.description = description
        transaction.date = date
        transaction.user_name = user_name
        transaction.is_private = is_private

        uploaded_files = flask.request.files.getlist("files")
        print(flask.request.files.getlist("files"))
        if any(uploaded_files):
            print(f"uploaded files {uploaded_files}")
            is_adding_correct_msg, yandex_link = transaction_worker.transaction_adding_YandexDisk(uploaded_files,
                                                                                                   transaction.id)
            flash(is_adding_correct_msg)

        db.session.add(transaction)
        db.session.commit()
        flash("Изменения внесены")

    else:
        transaction = Transaction.query.filter_by(id=id).first()
        transaction_yandex_disk_link = transaction_worker.get_link_yandex_disk_transaction(transaction.id)
        return render_template('transaction.html',
                               transaction=transaction, transaction_yandex_disk_link=transaction_yandex_disk_link)

    return redirect('/transactions')


@app.route('/transaction_copy', methods=['POST', 'GET'])
@login_required
def transaction_copy():
    if not current_user.is_authenticated:
        return redirect('/login')
    company_id = current_user.company_id

    if request.method == 'POST':
        amount = request.form['amount']
        description = request.form['description']
        date = datetime.date.today()
        if request.form['date']:
            date = request.form['date']
        user_name = request.form['user_name']
        # max_id = Transaction.query.with_entities(func.max(Transaction.id)).scalar()
        # if max_id is None:
        #     next_id = 1
        # else:
        #     next_id = max_id + 1
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
        return redirect('/login')
    company_id = current_user.company_id
    search = request.form['search']
    transactions, transactions_sum = transaction_worker.get_transactions(company_id,
                                                                         cur_user=current_user,
                                                                         search=search)
    print(f'search {search}')

    return render_template('transactions_div.html', transactions=transactions)


@app.route('/transaction_delete/<int:id>', methods=['POST', 'GET'])
@login_required
def transaction_delete(id):
    if not current_user.is_authenticated:
        return redirect('/login')
    flash("Запись удалена")
    transaction = Transaction.query.filter_by(id=id).one()
    db.session.delete(transaction)
    db.session.commit()

    return redirect('/transactions')


@app.route('/show_yandex_transaction_files/<int:transaction_id>', methods=['POST', 'GET'])
@login_required
def show_yandex_transaction_files(transaction_id):
    if not current_user.is_authenticated:
        return redirect('/login')
    images_path_list = transaction_worker.get_transactions_files(transaction_id)
    return render_template('transactions_files_div.html', images=images_path_list)
