# /// CATALOG ////////////
import flask
from app import app
from flask import flash, render_template, request, redirect, send_file
import pandas as pd


@app.route('/catalog', methods=['GET', 'POST'])
def catalog():
    """Обработка файла excel  - шапка нужна"""
    if request.method == 'POST':
        uploaded_files = flask.request.files.getlist("file")
        df_input_order = pd.read_excel(uploaded_files[0])
        art = df_input_order["Номенклатура"].tolist()
        size = df_input_order["Характеристика"].tolist()
        qt = df_input_order["Кол-во"].tolist()

        return render_template('catalog.html', art=art, size=size, qt=qt, tables=[
            df_input_order.to_html(classes='table table-bordered', header="true", index=False)])

    return render_template("upload_catalog.html")
