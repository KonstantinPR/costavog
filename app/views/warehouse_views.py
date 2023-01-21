from app import app
from flask import render_template, request, redirect, send_file
from urllib.parse import urlencode
from app.modules import img_cropper, io_output, img_processor, detailing_reports, base_module, API_WB, pdf_processor
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
    Вытягивает с яндекс.диска из эксель файлов поступления товаров
    """

    folder_path = app.config["YANDEX_FOLDER"]
    print(folder_path)
    folder_warehouse = app.config["WAREHOUSE"]
    path_to_files = f'{folder_path}/{folder_warehouse}'
    print(path_to_files)

    # specify the directory to iterate through
    directory = '/path/to/your/folder'

    # create an empty list to store the DataFrames of the Excel files
    excel_dfs = []

    # use os.walk() to iterate through the subfolders
    for root, dirs, files in os.walk(path_to_files):
        for file in files:
            # check if the file name contains what you need
            if 'Приход.xlsx' in file:
                if not file.startswith('.') and not file.startswith('~$'):
                    print(root)
                    print(file)
                    # read the Excel file into a DataFrame
                    print(os.path.join(root, file))
                    df = pd.read_excel(os.path.join(root, file))
                    # add the DataFrame to the list
                    excel_dfs.append(df)

    # concatenate the list of DataFrames into a single DataFrame
    if excel_dfs:
        result = pd.concat(excel_dfs)
        # print the concatenated DataFrame
        print(result)


    if request.method == 'POST':
        pass
    return render_template('upload_warehouse.html', doc_string=arrivals_of_products.__doc__)
