from app import app
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required
from app.models import db
from urllib.parse import urlencode
from app.modules import img_cropper, io_output
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

# /// YANDEX DISK ////////////


URL = 'https://cloud-api.yandex.net/v1/disk/resources'
TOKEN = 'AQAAAAAAoUNmAADLWy1QYoRObUDvk8auiY1pG2c'
headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': f'OAuth {TOKEN}'}


@app.route('/yandex_disc_take_photo', methods=['POST', 'GET'])
@login_required
def yandex_disc_take_photo():
    y = yadisk.YaDisk(token=TOKEN)
    id_folder = randrange(1000000)
    img_path = 'tmp_img_' + str(id_folder)
    images = []
    if not os.path.exists(img_path):
        os.makedirs(img_path)

    y.download("/img/MHS-BASCONI-AC3786-004-3-WHITE-BROWN-1.JPG",
               img_path + "/" + "MHS-BASCONI-AC3786-004-3-WHITE-BROWN-1.JPG")

    for filename in glob.glob(img_path + '/*.JPG'):
        im = Image.open(filename)
        images.append(im)

    print(images)

    images_zipped = img_cropper.crop_images(images)

    try:
        shutil.rmtree(img_path)
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))

    return send_file(images_zipped, attachment_filename='zip.zip', as_attachment=True)


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
