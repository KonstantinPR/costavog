# /// CATALOG ////////////
import flask
from werkzeug.datastructures import FileStorage
from io import BytesIO
from app import app
from flask import flash, render_template, request, redirect, send_file
import pandas as pd
from app.modules import text_handler, io_output


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

        multiply = int(request.form['multiply_number'])

        df = pd.read_fwf(file_txt)
        df_column = df.T.reset_index().set_axis(['Артикул']).T.reset_index(drop=True)
        df_multilpy = text_handler.names_multiply(df_column, multiply)
        df_output = io_output.io_output_txt_csv(df_multilpy)

        return send_file(
            df_output,
            as_attachment=True,
            attachment_filename='art.txt',
            mimetype='text/csv'
        )

    return render_template('upload_txt.html')
