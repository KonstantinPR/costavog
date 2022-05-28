from app import app
import flask
import requests
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required, current_user, login_user, logout_user
from app.models import Company, UserModel, Transaction, Task, Product, db
import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import desc
import pandas as pd
from io import BytesIO
from app.modules import io_output
import numpy as np
from sqlalchemy import create_engine
from urllib.parse import urlencode
import zipfile
from app.modules import discount, detailing, img_cropper
from base64 import encodebytes


@app.before_first_request
def create_all():
    db.create_all()


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/upload_img_crop', methods=['POST', 'GET'])
def upload_img_crop():
    imgs = []
    zip_buffer = str
    if request.method == "POST":
        upload_files = flask.request.files.getlist("files")
        print(upload_files)
        for file in upload_files:
            file_name = file.filename
            file = img_cropper.crop_img(file)
            print('file after crop_img: ' + str(file))
            img = io_output.io_img_output(file)

            imgs.append((file_name, img))

            print(imgs)

            zip_buffer = BytesIO()
            with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
                for file_name, data in imgs:
                    zip_file.writestr(file_name, data.getvalue())
            zip_buffer.seek(0)

            print(zip_buffer)

        return send_file(zip_buffer, attachment_filename='zip.zip', as_attachment=True)

        # return send_file(imgs[0], attachment_filename=file_name, as_attachment=True)

    return render_template('upload_img_crop.html')


# /// YANDEX DISK ////////////

@app.route('/download_yandex_disk_excel', methods=['POST', 'GET'])
@login_required
def download_yandex_disk_excel():
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
    public_key = 'https://yadi.sk/i/afeeYZOgnLkSnA'  # Сюда вписываете вашу ссылку

    # Получаем загрузочную ссылку
    final_url = base_url + urlencode(dict(public_key=public_key))
    response = requests.get(final_url)
    download_url = response.json()['href']

    download_response = requests.get(download_url)
    df = pd.read_excel(download_response.content)
    file = io_output.io_output(df)

    return send_file(file, attachment_filename="excel_yandex.xlsx", as_attachment=True)
