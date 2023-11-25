import re
from app import app, Company
from flask import render_template, request, redirect
from urllib.parse import urlencode
from app.modules import img_cropper, io_output, img_processor, base_module, API_WB, pdf_processor, yandex_disk_handler
import pandas as pd
import flask
from random import randrange
import shutil
from PIL import Image
import glob
from flask_login import login_required, current_user
import datetime
import yadisk
import os
import requests
from flask import send_file


# /// YandexDisk ////////////


# Return the zip file as a response
@app.route("/get_files_from_dir_ydisk", methods=['POST', 'GET'])
def get_files_from_dir_ydisk():
    """
    Взаимодействует с хранилищем яндекс.диска через API,
    Вытаскивает из указанной папки все найденные файлы в листе (название файла полное - включая расширение).
    На 15.02.2023 - рабочий вариант
    """
    if request.method == "POST":
        text_input = request.form["text_input"]
        dir_path = request.form["dir_path"]
        if not dir_path:
            dir_path = "ФОТОГРАФИИ/НОВЫЕ/2"
        file_name_list = text_input.split()
        zip_name = f"ziped_files_from_{dir_path}.zip"
        zip_name = re.sub('[\W]+\.', '_', zip_name)

        subfolder_names_list = yandex_disk_handler.get_subfolders_names(dir_path)
        all_file_urls_list = yandex_disk_handler.get_all_file_urls(subfolder_names_list, file_name_list, dir_path)
        zip_buffer = yandex_disk_handler.zip_buffer_files(all_file_urls_list, file_name_list)
        zip_buffer.seek(0)
        return send_file(zip_buffer, download_name=zip_name, as_attachment=True)

    return render_template("upload_yandex_disk_file.html", doc_string=get_files_from_dir_ydisk.__doc__)


@app.route('/get_stock_wb', methods=['POST', 'GET'])
@login_required
def get_stock_wb():
    """
    Достает все остатки с WB через API
    """

    if request.method == 'POST':
        df_all_cards = API_WB.get_wb_stock_api_extanded()
        df = io_output.io_output(df_all_cards)
        file_name = f'wb_api_stock_{str(datetime.datetime.now())}.xlsx'
        return send_file(df, download_name=file_name, as_attachment=True)
    return render_template('upload_get_info_wb.html', doc_string=get_info_wb.__doc__)


@app.route('/get_info_wb', methods=['POST', 'GET'])
@login_required
def get_info_wb():
    """
    Достает все актуальные карточки с WB через API
    """

    if request.method == 'POST':
        df_all_cards = API_WB.get_all_cards_api_wb()
        df = io_output.io_output(df_all_cards)
        file_name = f'wb_api_cards_{str(datetime.datetime.now())}.xlsx'
        return send_file(df, download_name=file_name, as_attachment=True)
    return render_template('upload_get_info_wb.html', doc_string=get_info_wb.__doc__)


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
        df = base_module.request_excel_to_df(flask.request)[0]
        # print(df)
        img_name_list_files = img_processor.download_images_from_yandex_to_folder(df)
        # print(img_name_list_files)
        path_pdf = pdf_processor.images_into_pdf_2(df)[0]
        pdf = os.path.abspath(path_pdf)
        return send_file(pdf, as_attachment=True)
    return render_template('upload_image_from_yadisk.html', doc_string=image_from_yadisk.__doc__)


@app.route('/yandex_disk_crop_images', methods=['POST', 'GET'])
@login_required
def yandex_disk_crop_images():
    if not current_user.is_authenticated:
        return redirect('/company_register')

    if request.method == 'POST':
        img_path_YandexDisk = request.form['yandex_path']
        company_id = current_user.company_id

        # create object that work with YandexDisk using TOKEN
        yandex_disk_token = Company.query.filter_by(id=current_user.company_id).one().yandex_disk_token
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json',
                   'Authorization': f'OAuth {yandex_disk_token}'}
        y = yadisk.YaDisk(token=yandex_disk_token)

        # for creating unique folder
        id_folder = randrange(1000000)
        img_path = 'tmp_img_' + str(id_folder)
        images = []

        if not os.path.exists(img_path):
            os.makedirs(img_path)

        # take names of all files on our directory in YandexDisk
        list_img_name = (list(y.listdir("" + str(img_path_YandexDisk))))

        # download all our images in temp folder
        for name_img in list_img_name:
            name = name_img['name']
            y.download("/" + str(img_path_YandexDisk) + "/" + name, img_path + "/" + name)

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

        return send_file(images_zipped, download_name='zip.zip', as_attachment=True)

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

    return send_file(file, download_name="excel_yandex.xlsx", as_attachment=True)
