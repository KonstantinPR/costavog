from app import app
from flask import render_template, request, redirect, send_file, flash
from urllib.parse import urlencode
from app.modules import warehouse_module, io_output, data_transforming_module
import pandas as pd
import flask
import requests
import yadisk
import os
from random import randrange
import shutil
from PIL import Image
from flask_login import login_required, current_user
import datetime


@app.route('/arrivals_of_products', methods=['POST', 'GET'])
@login_required
def arrivals_of_products():
    """
    Вытягивает файлы приходов из папок приходов на яндекс диске.
    Проходит по папкам имеющих следующую структуру.
    example path:
    path/PARTNERS/TRANSACTIONS/NUMBER_TRANSACTIONS/FILE
    example
    C/yadisk/test/КОНТРАГЕНТЫ/ПРИХОДЫ/N086_2022-01-21/Приход.xlsx
    Все приходы объединяются в один файл excel.
    Чекбокс - для вертикализирования размеров. Т.е из строки размеров, например 40, 42, 44, написанных в строку
    получим запись, где каждый размер с переносом строки.
    """

    if request.method == 'POST':
        # create an empty list to store the DataFrames of the Excel files

        list_paths_files = warehouse_module.preparing_paths()
        df = warehouse_module.df_from_list_paths_files_excel(list_paths_files)

        if not df:
            flash("DataFrame пустой, возможно неверно настроены пути или папки не существуют")
            return render_template('upload_warehouse.html', doc_string=arrivals_of_products.__doc__)

        df = pd.concat(df)

        if 'checkbox_is_vertical' in request.form:
            df = data_transforming_module.vertical_size(df)

        df_output = io_output.io_output(df)
        file_name = f"arrivals_of_products_on_{datetime.datetime.now().strftime('%Y-%m-%d')}.xlsx"
        return send_file(df_output, as_attachment=True, attachment_filename=file_name)

    return render_template('upload_warehouse.html', doc_string=arrivals_of_products.__doc__)

