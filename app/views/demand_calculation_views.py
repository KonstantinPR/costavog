import datetime
import os

import flask
import numpy as np
from flask_login import login_required
from werkzeug.datastructures import FileStorage

from app import app
from flask import render_template, request, send_file
import pandas as pd
import requests
from app.modules import yandex_disk_handler, detailing_reports, df_worker, io_output, img_processor, pdf_processor


def is_url_image(arts):
    art_paths = []
    for a in arts:
        ext = 'jpg'
        path_img = f'https://elenachezelle.ru/img-catalog/{a}-1.{ext}'
        r = requests.head(path_img)
        if not r.status_code == 200:
            ext = 'JPG'
        art_paths.append(path_img)

    print(art_paths)
    return art_paths


@app.route('/catalog', methods=['GET', 'POST'])
def catalog():
    """
    Не актуален на 08.12.2022. Заменен на Изображения с яндекс.диска.
    Обработка файла excel  - шапка нужна, Номенклатура, Характеристика, Кол-во"""
    if request.method == 'POST':
        uploaded_files = flask.request.files.getlist("file")
        df_input_order = pd.read_excel(uploaded_files[0])
        df_input_order.rename(columns={'Артикул поставщика': 'Номенклатура',
                                       'Размер': 'Характеристика',
                                       'Количество': 'Кол-во',
                                       }, inplace=True)
        arts = df_input_order["Номенклатура"].tolist()
        size = df_input_order["Характеристика"].tolist()
        qt = df_input_order["Кол-во"].tolist()

        print(arts)
        print(size)
        print(qt)
        # art_paths = is_url_image(arts)

        return render_template('catalog.html', arts=arts, size=size, qt=qt, tables=[
            df_input_order.to_html(classes='table table-bordered', header="true", index=False)])

    return render_template("upload_catalog.html", doc_string=catalog.__doc__, )


# @app.route('/catalog', methods=['GET', 'POST'])
# def catalog():
#     """Обработка файла excel  - шапка нужна, Номенклатура, Характеристика, Кол-во"""
#     if request.method == 'POST':
#         uploaded_files = flask.request.files.getlist("file")
#         df_input_order = pd.read_excel(uploaded_files[0])
#         df_input_order.rename(columns={'Артикул поставщика': 'Номенклатура',
#                                        'Размер': 'Характеристика',
#                                        'Количество': 'Кол-во',
#                                        }, inplace=True)
#         arts = df_input_order["Номенклатура"].tolist()
#         size = df_input_order["Характеристика"].tolist()
#         qt = df_input_order["Кол-во"].tolist()
#
#         print(arts)
#         print(size)
#         print(qt)
#         # art_paths = is_url_image(arts)
#         all_files = yandex_disk_handler.yadisk_get_files()
#
#         return send_file(all_files, as_attachment=True)
#
#         # return render_template('catalog.html', arts=arts, size=size, qt=qt, tables=[
#         #     df_input_order.to_html(classes='table table-bordered', header="true", index=False)])
#
#     return render_template("upload_catalog.html", doc_string=catalog.__doc__, )
@app.route('/demand_calculation_excel', methods=['POST', 'GET'])
@login_required
def demand_calculation_excel():
    """
    Присылает excel с потребностями.
    Напечатает те артикулы, которые передали в excel с шапкой vendorCode.
    В строке пишем через пробел подстроки, которые ищем в артикуле, которые хотим вывести в excel.
    Если передали файл - то приоритет у файла, строка работать не будет, Если файла нет - работает строка.
    """

    if request.method == 'POST':
        file_txt: FileStorage = request.files['file']

        search_string = str(request.form['search_string'])
        search_string_list = search_string.split()
        print(search_string_list)
        search_string_list = [x for x in search_string_list]
        print(search_string_list)

        search_string_first = search_string_list[0]

        df_all_cards = detailing_reports.get_all_cards_api_wb(textSearch=search_string_first)
        df_report, file_name = yandex_disk_handler.download_from_yandex_disk()
        print(file_name)
        df_wb_stock = detailing_reports.df_wb_stock_api()

        df = df_all_cards.merge(df_report, how='left', left_on='vendorCode', right_on='supplierArticle',
                                suffixes=("", "_drop"))
        df = df.merge(df_wb_stock, how='left',
                      left_on=['vendorCode', 'techSize'],
                      right_on=['supplierArticle', 'techSize'], suffixes=("", "_drop"))

        df.to_excel("df_all_actual_stock_and_art.xlsx")

        if file_txt.filename:
            df_input = pd.read_csv(file_txt, sep='	', names=['vendorCode'])
            df = df_input.merge(df, how='left', left_on='vendorCode', right_on='vendorCode')

        # df = pd.read_excel("df_output.xlsx")
        cols = ['vendorCode']
        # print(cols)
        m = pd.concat([df[cols].agg("".join, axis=1).str.contains(s) for s in search_string_list], axis=1).all(1)
        # print(m)
        df = df.drop_duplicates(subset=['vendorCode', 'techSize'])
        df = df[m].reset_index()

        df = df_worker.qt_to_order(df)
        df['techSize'] = pd.to_numeric(df['techSize'], errors='coerce').fillna(0).astype(np.int64)
        df['quantityFull'] = pd.to_numeric(df['quantityFull'], errors='coerce').fillna(0).astype(np.int64)
        df = df.sort_values(by=['Прибыль_sum', 'vendorCode', 'techSize'], ascending=True)
        df = df.reset_index(drop=True)
        df = io_output.io_output(df)
        file_name = f"demand_calculation_{str(datetime.date.today())}.xlsx"
        # df.to_excel("df_output.xlsx")


        return send_file(df, attachment_filename=file_name, as_attachment=True)
    return render_template('upload_demand_calculation_with_image_catalog.html',
                           doc_string=demand_calculation_excel.__doc__)


@app.route('/demand_calculation_with_image_catalog', methods=['POST', 'GET'])
@login_required
def demand_calculation_with_image_catalog():
    """
    Делает PDF каталог с потребностями. С фото артикула и другой информацией.
    Работает на локальном яндекс.диске.
    Напечатает те артикулы, которые передали в excel с шапкой vendorCode.
    В строке пишем через пробел подстроки, которые ищем в артикуле, которые хотим вывести в каталог.
    Если передали файл - то приоритет у файла, строка работать не будет, Если файла нет - работает строка.
    """

    if request.method == 'POST':
        file_txt: FileStorage = request.files['file']

        search_string = str(request.form['search_string'])
        search_string_list = search_string.split()
        print(search_string_list)
        search_string_list = [x for x in search_string_list]
        print(search_string_list)

        search_string_first = search_string_list[0]

        df_all_cards = detailing_reports.get_all_cards_api_wb(textSearch=search_string_first)
        df_report, file_name = yandex_disk_handler.download_from_yandex_disk()
        print(file_name)
        df_wb_stock = detailing_reports.df_wb_stock_api()

        df = df_all_cards.merge(df_report, how='left', left_on='vendorCode', right_on='supplierArticle',
                                suffixes=("", "_drop"))
        df = df.merge(df_wb_stock, how='left',
                      left_on=['vendorCode', 'techSize'],
                      right_on=['supplierArticle', 'techSize'], suffixes=("", "_drop"))

        df.to_excel("df_all_actual_stock_and_art.xlsx")

        if file_txt.filename:
            df_input = pd.read_csv(file_txt, sep='	', names=['vendorCode'])
            df = df_input.merge(df, how='left', left_on='vendorCode', right_on='vendorCode')

        # df = pd.read_excel("df_output.xlsx")
        cols = ['vendorCode']
        # print(cols)
        m = pd.concat([df[cols].agg("".join, axis=1).str.contains(s) for s in search_string_list], axis=1).all(1)
        # print(m)
        df = df.drop_duplicates(subset=['vendorCode', 'techSize'])
        df = df[m].reset_index()

        df = df_worker.qt_to_order(df)
        df['techSize'] = pd.to_numeric(df['techSize'], errors='coerce').fillna(0).astype(np.int64)
        df['quantityFull'] = pd.to_numeric(df['quantityFull'], errors='coerce').fillna(0).astype(np.int64)
        df = df.sort_values(by=['Прибыль_sum', 'vendorCode', 'techSize'], ascending=False)
        df = df.reset_index(drop=True)
        df.to_excel("df_output.xlsx")
        df_unique = pd.DataFrame(df['vendorCode'].unique(), columns=['vendorCode'])
        img_name_list_files = img_processor.download_images_from_yandex_to_folder(df_unique, art_col_name="vendorCode")
        path_pdf, no_photo_list = pdf_processor.images_into_pdf_2(df, art_col_name='vendorCode',
                                                                  size_col_name='techSize')
        pdf = os.path.abspath(path_pdf)

        return send_file(pdf, as_attachment=True)
    return render_template('upload_demand_calculation_with_image_catalog.html',
                           doc_string=demand_calculation_with_image_catalog.__doc__)
