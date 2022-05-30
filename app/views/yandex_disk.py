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


URL = app.config['URL']
TOKEN = app.config['TOKEN']
headers = {'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': f'OAuth {TOKEN}'}


@app.route('/yandex_disk_crop_images', methods=['POST', 'GET'])
@login_required
def yandex_disk_crop_images():
    # create object that work with yandex disk using TOKEN
    y = yadisk.YaDisk(token=TOKEN)

    # for creating unique folder
    id_folder = randrange(1000000)
    img_path = 'tmp_img_' + str(id_folder)
    images = []

    if not os.path.exists(img_path):
        os.makedirs(img_path)

    # take names of all files on our directory in yandex disk
    list_img_name = (list(y.listdir("/img")))

    # download all our images in temp folder
    for name_img in list_img_name:
        name = name_img['name']
        y.download("/img/" + name, img_path + "/" + name)

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
