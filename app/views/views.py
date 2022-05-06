from app import app
import flask
import requests
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required, current_user, login_user, logout_user
from app.models import Company, UserModel, Transaction, Task, Product, db
import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import desc
import pandas as pd
from io import BytesIO
import numpy as np
from sqlalchemy import create_engine
from urllib.parse import urlencode
from app.modules import discount, detailing


@app.before_first_request
def create_all():
    db.create_all()


def io_output(df):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer)
    writer.close()
    output.seek(0)
    return output


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# /// YANDEX DISK ////////////


@app.route('/download_yandex_disk_excel', methods=['POST', 'GET'])
@login_required
def download_yandex_disk_excel():
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
    public_key = 'https://yadi.sk/i/afeeYZOgnLkSnA'  # Сюда вписываете вашу ссылку

    # Получаем загрузочную ссылку
    final_url = base_url + urlencode(dict(public_key=public_key))
    response = requests.get(final_url)
    download_url = response.json()['href']

    download_response = requests.get(download_url)
    df = pd.read_excel(download_response.content)
    file = io_output(df)

    return send_file(file, attachment_filename="excel_yandex.xlsx", as_attachment=True)


# ///PRODUCTS////////////

@app.route('/upload_products', methods=['POST', 'GET'])
@login_required
def upload_products():
    if not current_user.is_authenticated:
        return redirect('/company_register')

    company_id = current_user.company_id

    if request.method == 'POST':
        uploaded_files = flask.request.files.getlist("file")
        df = pd.read_excel(uploaded_files[0])
        df.replace(np.NaN, "", inplace=True)
        df["company_id"] = company_id
        print(df)
        col_list = ['company_id', 'Артикул поставщика БАЗА', 'Себестоимость БАЗА']
        df = df[col_list].rename(columns={'Артикул поставщика БАЗА': 'article', 'Себестоимость БАЗА': 'net_cost'})

        engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])

        df2 = pd.read_sql(db.session.query(Product).filter_by(company_id=company_id).statement, db.session.bind)

        df3 = pd.concat([df, df2]).drop_duplicates(keep=False, subset=['company_id', 'article'])

        df3.to_sql('products', engine, if_exists='append', index=False)

        print(df3)

        df.to_sql('temp_table', engine, if_exists='replace')

        sql = "UPDATE products SET net_cost = temp_table.net_cost " \
              "FROM temp_table " \
              "WHERE products.article = temp_table.article " \
              f"AND products.company_id = {company_id}"

        with engine.begin() as conn:
            conn.execute(sql)

    return render_template('upload_products.html')


@app.route('/read_products', methods=['POST', 'GET'])
@login_required
def read_products():
    if not current_user.is_authenticated:
        return redirect('/company_register')

    company_id = current_user.company_id

    df = pd.read_sql(db.session.query(Product).statement, db.session.bind)
    df.replace(np.NaN, "", inplace=True)
    file = io_output(df)
    flash("Себестоимость товаров загружена")

    return send_file(file, attachment_filename="products.xlsx", as_attachment=True)


@app.route('/delete_products', methods=['POST', 'GET'])
@login_required
def delete_products():
    if not current_user.is_authenticated:
        return redirect('/company_register')

    company_id = current_user.company_id

    db.session.query(Product).filter_by(company_id=company_id).delete(synchronize_session='fetch')

    db.session.commit()
    flash(f"All products of {Company.company_name} was deleted")

    return render_template('upload_products.html')


@app.route('/delete_all_products', methods=['POST', 'GET'])
@login_required
def delete_all_products():
    if not current_user.is_authenticated:
        return redirect('/company_register')

    company_id = current_user.company_id

    db.session.query(Product).delete(synchronize_session='fetch')

    db.session.commit()
    flash("All products in database was deleted")

    return render_template('upload_products.html')


@app.route('/drop_products', methods=['POST', 'GET'])
@login_required
def drop_products():
    if not current_user.is_authenticated:
        return redirect('/company_register')

    company_id = current_user.company_id
    engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
    Product.__table__.drop(engine)

    flash("All table in database with products net cost was deleted")

    return render_template('upload_products.html')


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
        uploaded_files = flask.request.files.get("file")
        print(uploaded_files)

        is_net_cost = request.form.get('is_net_cost')
        print(is_net_cost == 'is_net_cost')

        if not uploaded_files:
            flash("Вы ничего не выбрали. Необходим zip архив с zip архивами, скаченными с сайта wb раздела детализаций")
            return render_template('upload_detailing.html')

        if is_net_cost:
            df_net_cost = pd.read_sql(db.session.query(Product).statement, db.session.bind)
            df_net_cost.replace(np.NaN, "", inplace=True)
        else:
            df_net_cost = False

        print(df_net_cost)

        df = detailing.zip_detail(uploaded_files, df_net_cost)

        file = io_output(df)

        return send_file(file, attachment_filename='report' + str(datetime.date.today()) + ".xlsx", as_attachment=True)

    return render_template('upload_detailing.html')
