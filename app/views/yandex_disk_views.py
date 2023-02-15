import io
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
import zipfile
from flask import send_file


# /// YANDEX DISK ////////////


# Return the zip file as a response
@app.route("/get_files_from_dir_ydisk", methods=['POST', 'GET'])
def get_files_from_dir_ydisk():
    """
    Взаимодействует с хранилищем яндекс.диска через API,
    Вытаскивает из указанной папки все найденные файлы в листе.
    На 15.02.2023 - рабочий вариант
    :return:
    """
    if request.method == "POST":
        y = yadisk.YaDisk(token=app.config['YANDEX_TOKEN'])
        text_input = request.form["text_input"]
        file_name_list = text_input.split()
        print(file_name_list)
        # dir_path = "ФОТОГРАФИИ/НОВЫЕ/2"
        dir_path = "ФОТОГРАФИИ/НОВЫЕ/2"
        zip_name = "zip.zip"

        subfolder_names = []
        all_file_urls = []
        for item in y.listdir(dir_path):
            if item.type == 'dir':
                subfolder_names.append(item.name)
        print(f"subfolder_names {subfolder_names}")

        for sub in reversed(subfolder_names):
            path = os.path.join(dir_path, sub).replace("\\", "/")
            file_urls = yandex_disk_handler.get_urls(path, file_name_list)
            all_file_urls += file_urls

        zip_buffer = yandex_disk_handler.zip_buffer_files(all_file_urls, file_name_list)

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
        df_all_cards = API_WB.get_wb_stock_api()
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
        path_pdf = pdf_processor.images_into_pdf_2(df)
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
