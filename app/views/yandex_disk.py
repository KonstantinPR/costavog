import app.modules.pdf_processor
from app import app
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required
from app.models import db
from urllib.parse import urlencode
from app.modules import img_cropper, io_output, img_processor, spec_modifiyer, detailing_reports, df_worker
import pandas as pd
import flask
import requests
import json
import yadisk
import os
from random import randrange
import shutil
from PIL import Image
import glob
from flask_login import login_required, current_user, login_user, logout_user
from app.modules import yandex_disk_handler, pdf_processor
from werkzeug.datastructures import FileStorage
from flask import send_from_directory
import numpy as np

import datetime

# /// YANDEX DISK ////////////


URL = app.config['URL']


@app.route('/get_info_wb', methods=['POST', 'GET'])
@login_required
def get_info_wb():
    """
    Достает все актуальные карточки с WB через API
    """

    if request.method == 'POST':
        df_all_cards = detailing_reports.get_all_cards_api_wb()
        df = io_output.io_output(df_all_cards)
        file_name = f'wb_api_cards_{str(datetime.datetime.now())}.xlsx'
        return send_file(df, attachment_filename=file_name, as_attachment=True)
    return render_template('upload_get_info_wb.html', doc_string=get_info_wb.__doc__)


@app.route('/image_from_yadisk_on_art', methods=['POST', 'GET'])
@login_required
def image_from_yadisk_on_art():
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
        df = df.sort_values(by=['vendorCode', 'techSize'], ascending=True)
        df.to_excel("df_output.xlsx")
        df_unique = pd.DataFrame(df['vendorCode'].unique(), columns=['vendorCode'])
        img_name_list_files = img_processor.download_images_from_yandex_to_folder(df_unique, art_col_name="vendorCode")
        path_pdf, no_photo_list = pdf_processor.images_into_pdf_2(df, art_col_name='vendorCode',
                                                                  size_col_name='techSize')
        pdf = os.path.abspath(path_pdf)

        return send_file(pdf, as_attachment=True)
    return render_template('upload_image_from_yadisk_on_art.html', doc_string=image_from_yadisk_on_art.__doc__)


@app.route('/image_from_yadisk', methods=['POST', 'GET'])
@login_required
def image_from_yadisk():
    """
    Делает PDF каталог с фото артикула и другой информацие по нашим потребностям о данном артикуле.
    Работает на локальном яндекс.диске.
    В шапке Артикул товара, Размер, Кол-во.
    """

    if request.method == 'POST':
        # file_txt: FileStorage = request.files['file']
        # df = pd.read_csv(file_txt, sep='	', names=['Article'])
        df = spec_modifiyer.request_to_df(flask.request)[0]
        # print(df)
        img_name_list_files = img_processor.download_images_from_yandex_to_folder(df)
        # print(img_name_list_files)
        path_pdf = app.modules.pdf_processor.images_into_pdf_2(df)
        pdf = os.path.abspath(path_pdf)
        return send_file(pdf, as_attachment=True)
    return render_template('upload_image_from_yadisk.html', doc_string=image_from_yadisk.__doc__)


@app.route('/yandex_disk_crop_images', methods=['POST', 'GET'])
@login_required
def yandex_disk_crop_images():
    if not current_user.is_authenticated:
        return redirect('/company_register')

    if request.method == 'POST':
        img_path_yandex_disk = request.form['yandex_path']
        company_id = current_user.company_id

        # create object that work with yandex disk using TOKEN
        yandex_disk_token = app.config['YANDEX_TOKEN']
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                   'Authorization': f'OAuth {yandex_disk_token}'}
        y = yadisk.YaDisk(token=yandex_disk_token)

        # for creating unique folder
        id_folder = randrange(1000000)
        img_path = 'tmp_img_' + str(id_folder)
        images = []

        if not os.path.exists(img_path):
            os.makedirs(img_path)

        # take names of all files on our directory in yandex disk
        list_img_name = (list(y.listdir("" + str(img_path_yandex_disk))))

        # download all our images in temp folder
        for name_img in list_img_name:
            name = name_img['name']
            y.download("/" + str(img_path_yandex_disk) + "/" + name, img_path + "/" + name)

        # take all img objects in list images
        for filename in glob.glob(img_path + '/*.JPG'):
            im = Image.open(filename)
            images.append(im)

        print(images)

        images_zipped = img_cropper.crop_images(images)

        # deleting directory with images that was zipped
        try:
            shutil.rmtree(img_path)
        except OSError as e:
            print("Error: %s - %s." % (e.filename, e.strerror))

        return send_file(images_zipped, attachment_filename='zip.zip', as_attachment=True)

    return redirect('/yandex_disk_crop_images')


@app.route('/download_yandex_disk_excel', methods=['POST', 'GET'])
@login_required
def download_yandex_disk_excel():
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
    public_key = 'https://yadi.sk/i/w5Aho2Ty-IA-Rg'  # Сюда вписываете вашу ссылку

    # Получаем загрузочную ссылку
    final_url = base_url + urlencode(dict(public_key=public_key))
    response = requests.get(final_url)
    download_url = response.json()['href']
    download_response = requests.get(download_url)
    df = pd.read_excel(download_response.content)
    file = io_output.io_output(df)

    return send_file(file, attachment_filename="excel_yandex.xlsx", as_attachment=True)
