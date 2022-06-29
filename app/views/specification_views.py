# /// CATALOG ////////////
import flask
from werkzeug.datastructures import FileStorage
from io import BytesIO
from app import app
from flask import flash, render_template, request, redirect, send_file
import pandas as pd
from app.modules import text_handler, io_output
import numpy as np
from flask_login import login_required, current_user, login_user, logout_user


@app.route('/image_name_multiply', methods=['GET', 'POST'])
@login_required
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


@app.route('/data_to_spec_merging', methods=['GET', 'POST'])
@login_required
def data_to_spec_merging():
    """Смерджить 2 excel файла - заполняемый файл и спецификацию"""
    name_on = "Артикул цвета"
    if request.method == 'POST':
        uploaded_files = flask.request.files.getlist("file")
        df_from = pd.read_excel(uploaded_files[0])
        df_to = pd.read_excel(uploaded_files[1])
        if request.form.get('name_on'):
            name_on = request.form.get('name_on')
        # df_from.replace(np.NaN, "", inplace=True)

        df_to = df_to.merge(df_from, copy=False, how='inner', on=name_on, suffixes=("", "_drop_column_on"))
        # Drop the duplicate columns
        df_to.drop([col for col in df_to.columns if '_drop_column_on' in col], axis=1, inplace=True)
        df_to.drop([col for col in df_to.columns if 'Unnamed:' in col], axis=1, inplace=True)
        df_to.set_index(name_on, inplace=True)
        df_excel = io_output.io_output(df_to)

        return send_file(
            df_excel,
            as_attachment=True,
            attachment_filename='spec.xlsx'
        )

    return render_template('upload_specs.html', name_on_default=name_on)
