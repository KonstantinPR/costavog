# /// CATALOG ////////////
import flask
from werkzeug.datastructures import FileStorage
from io import BytesIO
from app import app
from flask import flash, render_template, request, redirect, send_file
import pandas as pd
from app.modules import text_handler, io_output, spec_modifiyer, yandex_disk_handler
import numpy as np
from flask_login import login_required, current_user, login_user, logout_user


@app.route('/data_to_spec_wb_transcript', methods=['GET', 'POST'])
@login_required
def data_to_spec_wb_transcript():
    """Заполняется спецификация на основе справочников с яндекс.диска в таскере"""

    if request.method == 'POST':
        df_income_date = spec_modifiyer.request_to_df(flask.request)
        print(df_income_date)
        df_characters = yandex_disk_handler.get_excel_file_from_ydisk(app.config['CHARACTERS_PRODUCTS'])
        spec_type = spec_modifiyer.spec_definition(df_income_date)
        df_spec_example = yandex_disk_handler.get_excel_file_from_ydisk(app.config[spec_type])
        # df_art_prefixes = yandex_disk_handler.get_excel_file_from_ydisk(app.config['ECO_FURS_WOMEN'])
        df_colors = yandex_disk_handler.get_excel_file_from_ydisk(app.config['COLORS'])
        df_verticaling_sizes = spec_modifiyer.vertical_size(df_income_date)
        print(df_verticaling_sizes)
        # df_check_exist_art = spec_modifier.check_art_existing(df_verticaling_sizes)
        # df_merge_spec = spec_modifiyer.merge_spec(df_verticaling_sizes, df_spec_example, 'Артикул товара')
        df_art_prefixes_adding = spec_modifiyer.picking_prefixes(df_verticaling_sizes, df_spec_example)
        df_colors_adding = spec_modifiyer.picking_colors(df_art_prefixes_adding, df_colors)
        df_pattern_merge = spec_modifiyer.merge_spec(df_spec_example, df_colors_adding, 'Лекало')
        df_clear = spec_modifiyer.df_clear(df_pattern_merge)
        df_added_some_col = spec_modifiyer.col_adding(df_clear)
        # df_to_str = spec_modifiyer.col_str(df_added_some_col, ['Баркод товара'])

        print(df_verticaling_sizes)

        df_output = io_output.io_output(df_verticaling_sizes)
        return send_file(df_output, as_attachment=True, attachment_filename='test.xlsx', )

    return render_template('upload_data_to_spec_wb_transcript.html', doc_string=data_to_spec_wb_transcript.__doc__)


@app.route('/image_name_multiply', methods=['GET', 'POST'])
@login_required
def image_name_multiply():
    """Обработка файла txt - размножит названия артикулей с префиксом -1, -2 и т.д требуемое кол-во раз"""
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
    """Смержить 2 excel файла - заполняемый произвольный файл (можно с картинками) и спецификацию"""
    name_on = "Артикул цвета"
    barcode_column_name = "Штрихкод товара"
    if request.method == 'POST':
        uploaded_files = flask.request.files.getlist("file")
        df_from = pd.read_excel(uploaded_files[0])
        df_to = pd.read_excel(uploaded_files[1])
        if request.form.get('name_on'):
            name_on = request.form.get('name_on')
            if ',' in name_on:
                name_on = name_on.split(',')
        # df_from.replace(np.NaN, "", inplace=True)

        df_to = df_to.merge(df_from, copy=False, how='inner', on=name_on, suffixes=("", "_drop_column_on"))
        # Drop the duplicate columns
        df_to.drop([col for col in df_to.columns if '_drop_column_on' in col], axis=1, inplace=True)
        df_to.drop([col for col in df_to.columns if 'Unnamed:' in col], axis=1, inplace=True)
        df_to.set_index(name_on, inplace=True)
        df_to[barcode_column_name] = df_to[barcode_column_name].apply(lambda x: '{:d}'.format(x))
        df_excel = io_output.io_output(df_to)

        return send_file(
            df_excel,
            as_attachment=True,
            attachment_filename='spec.xlsx'
        )

    return render_template('upload_specs.html', name_on_default=name_on)
