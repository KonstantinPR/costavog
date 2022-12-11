# /// DATA_TRANSFORMING ////////////
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
    """
    Делает вертикальными размеры записанные в строку в одной ячейке.
    В первой колонке - Артикул товара, вторая - Размеры, третья - На фото.
    Если нет колонки кол-во, то - добавится 1 на каждый артикул.

    В поле можно прописать размеры, которые будут созданы, например:
    40 56 2 - создаст размеры 40 42 44 46 ... 56 (т.е с 40 по 56 включительно с шагом 2)
    (Если в файле есть поле Размеры - то поле игнорируется)
    """

    if request.method == 'POST':
        size_col_name = "Размеры"
        df = spec_modifiyer.request_to_df(flask.request)
        df = df[0]
        df = spec_modifiyer.str_input_to_full_str(df, request, size_col_name, input_name='size_forming',
                                                  html_template='upload_vertical_sizes.html')
        df = spec_modifiyer.vertical_size(df)
        is_photo_col_name = 'На фото'
        if is_photo_col_name in df.columns:
            df = spec_modifiyer.to_keep_for_photo(df)
        df_output = io_output.io_output(df)
        file_name = f"vertical_sizes_{str(datetime.datetime.now())}.xlsx"
        return send_file(df_output, as_attachment=True, attachment_filename=file_name)

    return render_template('upload_vertical_sizes.html', doc_string=vertical_sizes.__doc__)


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
