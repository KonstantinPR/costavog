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
from app.modules import io_output
import time



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

        sql_temp_del = "DELETE FROM temp_table"

        with engine.begin() as conn:
            conn.execute(sql_temp_del)

    flash("Себестоимость товаров загружена")

    return render_template('upload_products.html')


@app.route('/test/<id>', methods=['POST', 'GET'])
@login_required
def test(id):
    return render_template('test.html', id=id)


@app.route('/read_products', methods=['POST', 'GET'])
@login_required
def read_products():
    if not current_user.is_authenticated:
        return redirect('/company_register')

    company_id = current_user.company_id

    df = pd.read_sql(db.session.query(Product).statement, db.session.bind)
    df.replace(np.NaN, "", inplace=True)
    file = io_output.io_output(df)
    flash("Себестоимость товаров скачана в excel")

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


@app.route('/drop_products', methods=[' POST', 'GET'])
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
        file = io_output.io_output(df)

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

        is_get_stock = request.form.get('is_get_stock')

        if is_get_stock:
            df_stock = detailing.get_wb_stock()
            df = df.merge(df_stock, left_on='Артикул поставщика', right_on='supplierArticle')

        file = io_output.io_output(df)

        flash("Отчет успешно выгружен в excel файл")
        return send_file(file, attachment_filename='report_detailing' + str(datetime.date.today()) + ".xlsx",
                         as_attachment=True)

    return render_template('upload_detailing.html')


@app.route('/get_wb_stock', methods=['POST', 'GET'])
@login_required
def get_wb_stock():
    if not current_user.is_authenticated:
        return redirect('/company_register')

    df = detailing.get_wb_stock()
    file = io_output.io_output(df)

    return send_file(file, attachment_filename='report' + str(datetime.date.today()) + ".xlsx", as_attachment=True)


@app.route('/get_dynamic_sales', methods=['POST', 'GET'])
@login_required
def get_dynamic_sales():
    """To get speed of sales for all products in period"""
    if not current_user.is_authenticated:
        return redirect('/company_register')
    if request.method == 'POST':
        if request.form.get('date_from'):
            date_from = request.form.get('date_from')
        else:
            date_from = datetime.datetime.today() - datetime.timedelta(days=app.config['DAYS_STEP_DEFAULT'])


        print(date_from)

        if request.form.get('date_end'):
            date_end = request.form.get('date_end')
        else:
            date_end = datetime.datetime.today()

        print(date_end)

        if request.form.get('days_step'):
            days_step = request.form.get('days_step')
        else:
            days_step = app.config['DAYS_STEP_DEFAULT']
        t = time.process_time()
        print(time.process_time() - t)
        # df_sales_wb_api = detailing.get_wb_sales_api(date_from, days_step)
        df_sales_wb_api = detailing.get_wb_sales_realization_api(date_from, date_end, days_step)
        print(time.process_time() - t)
        file = io_output.io_output(df_sales_wb_api)
        print(time.process_time() - t)
        return send_file(file,
                         attachment_filename='report' + str(datetime.date.today()) + str(datetime.time()) + ".xlsx",
                         as_attachment=True)

    return render_template('upload_get_dynamic_sales.html')
