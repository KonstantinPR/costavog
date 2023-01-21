from app import app
from flask import render_template, request, redirect, send_file, flash
from urllib.parse import urlencode
from app.modules import warehouse_module, io_output
import pandas as pd
import flask
import requests
import yadisk
import os
from random import randrange
import shutil
from PIL import Image
import glob
from flask_login import login_required, current_user

import datetime

# /// YANDEX DISK ////////////


URL = app.config['URL']


@app.route('/arrivals_of_products', methods=['POST', 'GET'])
@login_required
def arrivals_of_products():
    """
    Вытягивает файлы приходов из папок приходов на яндекс диске
    """

    if request.method == 'POST':

        # create an empty list to store the DataFrames of the Excel files

        path_to_files = f'{app.config["YANDEX_FOLDER"]}/{app.config["WAREHOUSE"]}'
        list_paths_files = glob.glob(path_to_files + '/*/Приход*.xlsx', recursive=True)
        df = warehouse_module.df_from_list_paths_files_excel(list_paths_files)

        # # use os.walk() to iterate through the subfolders - the second var
        # for root, dirs, files in os.walk(path_to_files):
        #     for file in files:
        #         # check if the file name contains what you need
        #         if 'Приход' in file:
        #             if not file.startswith('.') and not file.startswith('~$'):
        #                 print(root)
        #                 print(file)
        #                 # read the Excel file into a DataFrame
        #                 print(os.path.join(root, file))
        #                 df = pd.read_excel(os.path.join(root, file))
        #                 # add the DataFrame to the list
        #                 excel_dfs.append(df)

        # concatenate the list of DataFrames into a single DataFrame
        if df:
            result = pd.concat(df)
            # print the concatenated DataFrame
            print(result)
            df_output = io_output.io_output(result)
            file_name = f"arrivals_of_products_on_{datetime.datetime.now().strftime('%Y-%m-%d')}.xlsx"
            return send_file(df_output, as_attachment=True, attachment_filename=file_name)

    # flash("Нет файлов приходов. Проверье настройки путей до папок приходов.")

    return render_template('upload_warehouse.html', doc_string=arrivals_of_products.__doc__)
