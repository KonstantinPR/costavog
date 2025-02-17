# /// DATA_TRANSFORMING ////////////

import flask
import logging
from app import app
from flask import flash, render_template, request, send_file
from app.modules import text_handler, io_output, spec_modifiyer, yandex_disk_handler, base_module, \
    data_transforming_module, request_handler
from app.modules.decorators import timing_decorator
from flask_login import login_required
import datetime
from random import randrange
import pandas as pd


@app.route('/color_translate', methods=['GET', 'POST'])
@login_required
def color_translate():
    """
    Выбирает из каждого артикула в передаваемом текстовом файле цвет на английском и переводит его на русский.
    На входе - txt с артикулами без шапки.
    """

    if request.method == 'POST':
        col_name = 'Артикул'
        df = request_handler.to_df(request, input_column=col_name)

        df_colors = yandex_disk_handler.get_excel_file_from_ydisk(app.config['COLORS'])
        df = spec_modifiyer.picking_colors(df, df_colors, df_col_name=col_name)
        df_output = io_output.io_output(df)
        file_name = f"colors_rus_{str(datetime.datetime.now())}.xlsx"
        return send_file(df_output, as_attachment=True, download_name=file_name)

    return render_template('upload_color_translate.html', doc_string=color_translate.__doc__)


@app.route('/vertical_sizes', methods=['GET', 'POST'])
@login_required
def vertical_sizes():
    """
    Делает вертикальными размеры записанные в строку в одной ячейке и откладывает на фотосессию новинки.
    В первой колонке - Артикул товара, вторая - Размеры, третья - На фото.
    Если нет колонки кол-во, то - добавится 1 на каждый артикул.
    Если нет колонки 'На фото' - тогда программа вытащит через апи все артикулы с WB и если
    там нет какого-то артикула (т.е. он новы) то напротив него поставится 1 (выбирется подходящий размер).

    В поле input можно прописать размеры, которые будут созданы, например:
    40 56 2 - создаст размеры 40 42 44 46 ... 56 (т.е с 40 по 56 включительно с шагом 2)
    (Если в файле есть поле Размеры - то поле игнорируется)
    """

    if request.method == 'POST':
        main_col = ['Артикул товара', 'Размер', 'Кол-во', 'На фото']
        size_col_name = "Размеры"
        df = base_module.request_excel_to_df(flask.request)
        df = df[0]
        df = data_transforming_module.str_input_to_full_str(df, request, size_col_name,
                                                            input_name='size_forming',
                                                            html_template='upload_vertical_sizes.html')

        is_photo_col_name = 'На фото'
        all_cards_wb = "all_cards_wb.xlsx"
        if is_photo_col_name in df.columns:
            df = data_transforming_module.vertical_size(df)
            df = data_transforming_module.to_keep_for_photo(df)
        else:
            # df_all_cards_api_wb = detailing_api_module.get_all_cards_api_wb()

            df_all_cards_api_wb = pd.read_excel(all_cards_wb)
            df_all_cards_api_wb = df_all_cards_api_wb.drop_duplicates(subset=['vendorCode'])
            df_photo = df.merge(df_all_cards_api_wb,
                                left_on=['Артикул товара'],
                                right_on=['vendorCode'],
                                how='outer',
                                suffixes=['', '_'],
                                indicator=True)

            random_suffix = f'_col_on_drop_{randrange(10)}'
            df = df.merge(df_photo, how='left', on='Артикул товара', suffixes=('', random_suffix))
            df = df.drop(columns=[x for x in df.columns if random_suffix in x])
            df['На фото'] = [1 if x == 'left_only' else "" for x in df['_merge']]
            df = data_transforming_module.vertical_size(df)
            df = data_transforming_module.to_keep_for_photo(df)
            df = df[main_col]

        df_output = io_output.io_output(df)
        file_name = f"vertical_sizes_{str(datetime.datetime.now())}.xlsx"
        return send_file(df_output, as_attachment=True, download_name=file_name)

    return render_template('upload_vertical_sizes.html', doc_string=vertical_sizes.__doc__)


@app.route('/image_name_multiply', methods=['GET', 'POST'])
@login_required
@timing_decorator
def image_name_multiply():
    """Process a DataFrame column to multiply strings based on the given parameters."""

    if request.method == 'POST':
        input_column = 'Артикул'
        df = request_handler.to_df(request, input_column=input_column)

        if not request.form['multiply_number']:
            flash("Сколько фото делать то будем? Поле пустое")
            return render_template('upload_image_name_multiply.html')

        if not request.form['start_from']:
            start_from = 1
        else:
            start_from = int(request.form['start_from'])

        multiply = int(request.form['multiply_number'])

        df_multiply = text_handler.names_multiply(df, multiply, input_column=input_column, start_from=start_from)
        # print(df_multiply)
        df_output = io_output.io_output_txt_csv(df_multiply)
        return send_file(df_output, as_attachment=True, download_name='art.txt', mimetype='text/csv')
    return render_template('upload_image_name_multiply.html', doc_string=image_name_multiply.__doc__)
