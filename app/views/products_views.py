import app.modules.API_WB
import logging
from app import app
import flask
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required, current_user
from app.models import Company, Product, db
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from app.modules import discount, API_WB
from app.modules import io_output


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
        # print(df)
        col_list = ['company_id', 'Артикул поставщика БАЗА', 'Себестоимость БАЗА']
        for col_name in col_list:
            if col_name in df.columns:
                df = df[col_list].rename(
                    columns={'Артикул поставщика БАЗА': 'article', 'Себестоимость БАЗА': 'net_cost'})

        df["company_id"] = company_id

        engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])

        df2 = pd.read_sql(db.session.query(Product).filter_by(company_id=company_id).statement, db.session.bind)

        df3 = pd.concat([df, df2]).drop_duplicates(keep=False, subset=['company_id', 'article'])

        df3.to_sql('products', engine, if_exists='append', index=False)

        # print(df3)

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


# @app.route('/test/<id>', methods=['POST', 'GET'])
# @login_required
# def test(id):
#     return render_template('test.html', id=id)


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

    return send_file(file, download_name="products.xlsx", as_attachment=True)


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
    """on 06/05/2023 need to be checked, not used for a long time"""
    if not current_user.is_authenticated:
        return redirect('/company_register')

    if request.method == 'POST':
        uploaded_files = flask.request.files.getlist("file")
        df = pd.read_excel(uploaded_files[0])
        df.replace(np.NaN, "", inplace=True)
        df_products = pd.read_sql(db.session.query(Product).statement, db.session.bind)
        df = discount.discount(df, df_products)
        file = io_output.io_output(df)

        return send_file(file, download_name="excel.xlsx", as_attachment=True)

    return render_template('upload_turnover.html')


