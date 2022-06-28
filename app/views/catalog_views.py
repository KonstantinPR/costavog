# /// CATALOG ////////////
import flask
from werkzeug.datastructures import FileStorage
from io import BytesIO
from app import app
from flask import flash, render_template, request, redirect, send_file
import pandas as pd
from app.modules.io_output import io_output
import numpy as np


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


@app.route('/image_name_multiply', methods=['GET', 'POST'])
def image_name_multiply():
    """Обработка файла txt"""
    if request.method == 'POST':
        file_txt: FileStorage = request.files['file']

        if not request.files['file']:
            flash("Не приложен файл")
            return render_template('upload_txt.html')

        if not request.form['multiply_number']:
            flash("Сколько фото делать то будем? Поле пустое")
            return render_template('upload_txt.html')
        print(request.form['multiply_number'])
        multiply = int(request.form['multiply_number'])
        multiply_number = multiply + 1

        df = pd.read_fwf(file_txt)
        df = df.T.reset_index().set_axis(['Артикул']).T.reset_index(drop=True)

        print(df)

        list_of_multy = []
        for art in df['Артикул']:
            for n in range(1, multiply_number):
                art_multy = f'{art}-{n}'
                list_of_multy.append(art_multy)
        df = pd.DataFrame(list_of_multy)

        towrite = BytesIO()
        df = df.to_csv(header=False, index=False).encode()
        towrite.write(df)
        towrite.seek(0)
        print(towrite)

        return send_file(
            towrite,
            as_attachment=True,
            attachment_filename='art.txt',
            mimetype='text/csv'
        )

    return render_template('upload_txt.html')
