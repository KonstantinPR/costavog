# /// CATALOG ////////////
import time

import flask
from app import app
from flask import flash, render_template, request, send_file
import pandas as pd
from app.modules import text_handler, io_output, spec_modifiyer, yandex_disk_handler, df_worker
from flask_login import login_required
import datetime


@app.route('/color_translate', methods=['GET', 'POST'])
@login_required
def color_translate():
    """
    Выбирает из каждого артикула в передаваемом текстовом файле цвет на английском и переводит его на русский.
    На входе - txt с артикулами без шапки.
    """

    if request.method == 'POST':
        col_name = 'Артикул'
        df = io_output.io_txt_request(request,
                                      name_html='upload_image_name_multiply.html',
                                      inp_name='file',
                                      col_name=col_name)

        df_colors = yandex_disk_handler.get_excel_file_from_ydisk(app.config['COLORS'])
        df = spec_modifiyer.picking_colors(df, df_colors, df_col_name=col_name)
        df_output = io_output.io_output(df)
        file_name = f"colors_rus_{str(datetime.datetime.now())}.xlsx"
        return send_file(df_output, as_attachment=True, attachment_filename=file_name)

    return render_template('upload_color_translate.html', doc_string=color_translate.__doc__)


@app.route('/vertical_sizes', methods=['GET', 'POST'])
@login_required
def vertical_sizes():
    """Делает вертикальными размеры записанные в строку в одной ячейке вертикальными в колонку.
    В первой колонке - Артикул товара, вторая - Размеры, На фото, если нет колонки кол-во, то - добавится с 1 каждый артикул"""

    if request.method == 'POST':
        df = spec_modifiyer.request_to_df(flask.request)
        df = df[0]
        print(df)
        df = spec_modifiyer.vertical_size(df)
        is_photo_col_name = 'На фото'
        if is_photo_col_name in df.columns:
            df = spec_modifiyer.to_keep_for_photo(df)
        df_output = io_output.io_output(df)
        file_name = f"vertical_sizes_{str(datetime.datetime.now())}.xlsx"
        return send_file(df_output, as_attachment=True, attachment_filename=file_name)

    return render_template('upload_vertical_sizes.html', doc_string=vertical_sizes.__doc__)


@app.route('/data_to_spec_wb_transcript', methods=['GET', 'POST'])
@login_required
def data_to_spec_wb_transcript():
    """
    Заполняется спецификация на основе справочников с яндекс.диска в таскере.
    На входе excel с шапкой: Артикул товара, Размеры (размерный ряд товара), Цена - не обящатеьльна
    """

    if request.method == 'POST':
        df_income_date = spec_modifiyer.request_to_df(flask.request)
        df_income_date = df_income_date[0]
        # df_characters = yandex_disk_handler.get_excel_file_from_ydisk(app.config['CHARACTERS_PRODUCTS'])
        spec_type = spec_modifiyer.spec_definition(df_income_date)
        print(spec_type)
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
        return send_file(df_output, as_attachment=True, attachment_filename='spec_created.xlsx', )

    return render_template('upload_data_to_spec_wb_transcript.html', doc_string=data_to_spec_wb_transcript.__doc__)


@app.route('/image_name_multiply', methods=['GET', 'POST'])
@login_required
def image_name_multiply():
    """Обработка файла txt - размножит названия артикулей с префиксом -1, -2 и т.д требуемое кол-во раз"""

    if request.method == 'POST':
        df_column = io_output.io_txt_request(request,
                                             name_html='upload_image_name_multiply.html',
                                             inp_name='file',
                                             col_name='Артикул')
        if not request.form['multiply_number']:
            flash("Сколько фото делать то будем? Поле пустое")
            return render_template('upload_image_name_multiply.html')
        multiply = int(request.form['multiply_number'])
        df_multilpy = text_handler.names_multiply(df_column, multiply)
        df_output = io_output.io_output_txt_csv(df_multilpy)
        return send_file(df_output, as_attachment=True, attachment_filename='art.txt', mimetype='text/csv')
    return render_template('upload_image_name_multiply.html')


@app.route('/data_to_spec_merging', methods=['GET', 'POST'])
@login_required
def data_to_spec_merging():
    """Смержить 2 excel файла, порядок в алфавитном - в первом оставляем, если уже были"""
    if request.method == 'POST':
        uploaded_files = flask.request.files.getlist("file")
        df_from = pd.read_excel(uploaded_files[0])
        df_to = pd.read_excel(uploaded_files[1])
        df = spec_modifiyer.merge_spec(df_to, df_from, left_on='Артикул товара', right_on='Артикул товара')
        df = io_output.io_output(df)

        return send_file(df, as_attachment=True, attachment_filename='spec.xlsx')

    return render_template('upload_specs.html')


@app.route('/take_off_boxes', methods=['GET', 'POST'])
@login_required
def take_off_boxes():
    """
    На 08.12.2022 - не актуально - функция сложна в реализации и редко используется.
    Удаляет коробки с товарами, которых много, на входе эксель таблица с артикулами и кол-вом ограничителем,
    шапка первого: Артикул товара (полный с размером), второго: Артикул, третьего: Можно
    """
    if request.method == 'POST':
        dfs = spec_modifiyer.request_to_df(flask.request)
        df = spec_modifiyer.merge_spec(dfs[0], dfs[1], how='left')
        print(df)
        df = df_worker.df_take_off_boxes(df)
        df = io_output.io_output(df)
        return send_file(df, as_attachment=True, attachment_filename='table_take_off_boxes.xlsx')

    return render_template('upload_take_off_boxes.html', doc_string=take_off_boxes.__doc__)
