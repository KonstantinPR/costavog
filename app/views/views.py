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
from app.modules import discount, detailing


@app.before_first_request
def create_all():
    db.create_all()



def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


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

