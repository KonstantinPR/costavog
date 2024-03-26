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


@app.route('/get_storage_data_route', methods=['POST', 'GET'])
@login_required
def get_storage_cost_route():
    """
    Достает все актуальные карточки с WB через API
    """

    if request.method == 'POST':
        df_all_cards = API_WB.get_storage_data()
        print(f"df {df_all_cards}")
        df = io_output.io_output(df_all_cards)
        file_name = f'storage_data_{str(datetime.datetime.now())}.xlsx'
        return send_file(df, download_name=file_name, as_attachment=True)
    return render_template('upload_get_info_wb.html', doc_string=get_info_wb.__doc__)
