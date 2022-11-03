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
        # df_characters = yandex_disk_handler.get_excel_file_from_ydisk(app.config['CHARACTERS_PRODUCTS'])
        spec_type = spec_modifiyer.spec_definition(df_income_date)
        df_spec_example = yandex_disk_handler.get_excel_file_from_ydisk(app.config[spec_type],
                                                                        to_str=['Лекало', 'Префикс'])
        # df_art_prefixes = yandex_disk_handler.get_excel_file_from_ydisk(app.config['ECO_FURS_WOMEN'])
        df_colors = yandex_disk_handler.get_excel_file_from_ydisk(app.config['COLORS'])
        df_verticaling_sizes = spec_modifiyer.vertical_size(df_income_date)
        # df_check_exist_art = spec_modifier.check_art_existing(df_verticaling_sizes)
        # df_merge_spec = spec_modifiyer.merge_spec(df_verticaling_sizes, df_spec_example, 'Артикул товара')
        df_art_prefixes_adding = spec_modifiyer.picking_prefixes(df_verticaling_sizes, df_spec_example)
        df_colors_adding = spec_modifiyer.picking_colors(df_art_prefixes_adding, df_colors)
        df_pattern_merge = spec_modifiyer.merge_spec(df_colors_adding, df_spec_example, 'Лекало', 'Лекало')
        df_clear = spec_modifiyer.df_clear(df_pattern_merge)
        df_added_some_col = spec_modifiyer.col_adding(df_clear)
        df_to_str = spec_modifiyer.col_str(df_added_some_col, ['Баркод товара'])

        print(df_to_str)

        df_output = io_output.io_output(df_to_str)
        return send_file(df_output, as_attachment=True, attachment_filename='test.xlsx', )

    return render_template('upload_data_to_spec_wb_transcript.html', doc_string=data_to_spec_wb_transcript.__doc__)


@app.route('/image_name_multiply', methods=['GET', 'POST'])
@login_required
def image_name_multiply():
    """Обработка файла txt - размножит названия артикулей с префиксом -1, -2 и т.д требуемое кол-во раз"""

    if request.method == 'POST':
        df_column = io_output.io_txt_request(request, inp_name='file', col_name='Артикул')
        if not request.form['multiply_number']:
            flash("Сколько фото делать то будем? Поле пустое")
            return render_template('upload_txt.html')
        multiply = int(request.form['multiply_number'])
        df_multilpy = text_handler.names_multiply(df_column, multiply)
        df_output = io_output.io_output_txt_csv(df_multilpy)
        return send_file(df_output, as_attachment=True, attachment_filename='art.txt', mimetype='text/csv')
    return render_template('upload_txt.html')


@app.route('/data_to_spec_merging', methods=['GET', 'POST'])
@login_required
def data_to_spec_merging():
    """Смержить 2 excel файла, порядок в алфавитном - в первом оставляем, если уже были"""
    if request.method == 'POST':
        uploaded_files = flask.request.files.getlist("file")
        df_from = pd.read_excel(uploaded_files[0])
        df_to = pd.read_excel(uploaded_files[1])
        df = spec_modifiyer.merge_spec(df_to, df_from, left_on='Артикул', right_on='nm_id')
        df = io_output.io_output(df)

        return send_file(df, as_attachment=True, attachment_filename='spec.xlsx')

    return render_template('upload_specs.html')


@app.route('/take_off_boxes', methods=['GET', 'POST'])
@login_required
def take_off_boxes():
    """Удаляет коробки с товарами, которых много, на входе эксель таблица с артикулами и кол-вом ограничителем"""
    if request.method == 'POST':
        dfs = spec_modifiyer.request_to_df(flask.request)
        dfs = io_output.io_output(dfs)
        return send_file(dfs, as_attachment=True, attachment_filename='table_take_off_boxes.xlsx')

    return render_template('upload_take_off_boxes.html', doc_string=take_off_boxes.__doc__)
