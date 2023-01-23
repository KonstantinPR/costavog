import flask
from app import app
import pandas as pd
from app.modules import decorators
import numpy as np
from random import randrange
from flask import flash, render_template
import inspect
import os
import glob
import openpyxl

import json
import requests
import pandas as pd
import io
from io import BytesIO


def search_file(api_key, path, filename):
    # Yandex.Disk API endpoint for listing files
    list_url = 'https://cloud-api.yandex.net/v1/disk/resources'

    # Your Yandex.Disk API token
    api_token = app.config['YANDEX_TOKEN']

    # The folder you want to search in
    folder = 'ПОСТАВЩИКИ/TEST/КОВРИГИНА/Приходы'

    # The file name you want to search for
    file_name = 'Приход.xlsx'

    # List to store the paths of the files you're looking for
    paths_seek_files = []

    # Get a list of all files and folders in the specified folder
    params = {'path': folder}
    headers = {'Authorization': f'OAuth {api_token}'}
    response = requests.get(list_url, params=params, headers=headers)
    print(f"response {response}")
    files = response.json()['_embedded']['items']

    # Iterate over the files and folders
    for item in files:
        # if file has the name that you're looking for
        if item['name'] == file_name:
            paths_seek_files.append(item['path'])
        elif item['type'] == 'dir':
            sub_folder_path = item['path']
            # get all files in subfolder
            sub_folder_params = {'path': sub_folder_path}
            sub_folder_response = requests.get(list_url, params=sub_folder_params, headers=headers)
            sub_folder_files = sub_folder_response.json()['_embedded']['items']
            for sub_file in sub_folder_files:
                if sub_file['name'] == file_name:
                    paths_seek_files.append(sub_file['path'])

    # for recursive
    # def find_files(folder_path, file_name):
    #     # get all files in folder
    #     params = {'path': folder_path}
    #     response = requests.get(list_url, params=params, headers=headers)
    #     files = response.json()['_embedded']['items']
    #     for item in files:
    #         if item['name'] == file_name:
    #             paths_seek_files.append(item['path'])
    #         elif item['type'] == 'dir':
    #             sub_folder_path = item['path']
    #             find_files(sub_folder_path, file_name)
    #
    # find_files(folder, file_name)

    # iterate over the paths of the files you're looking for and convert them to a DataFrame
    dfs = []
    for path in paths_seek_files:
        # get file's metadata
        params = {'path': path}
        response = requests.get(list_url, params=params, headers=headers)
        file_metadata = response.json()
        file_url = file_metadata['file']
        file_response = requests.get(file_url)
        df = pd.read_excel(file_response.content)
        dfs.append(df)
    final_df = pd.concat(dfs)
    print(final_df)


SUBFOLDERS = {
    """
    example path: path/PARTNERS/TRANSACTIONS/NUMBER_TRANSACTIONS/FILE
    example C/yadisk/test/КОНТРАГЕНТЫ/ПРИХОДЫ/N086_2022-01-21/Приход.xlsx
    """
    'FILE': 0,
    'NUMBER_TRANSACTIONS': 1,
    'TRANSACTIONS': 2,
    'PARTNERS': 3,

}


def preparing_paths():
    path_to_files = f'{app.config["YANDEX_FOLDER"]}/{app.config["WAREHOUSE"]}'
    list_paths_files = glob.glob(path_to_files + '/*' + app.config["ARRIVALS"] + '/*/' + 'Приход.xlsx',
                                 recursive=True)
    return list_paths_files


def get_parent_folder_path(file_path, steps):
    for i in range(steps):
        file_path = os.path.dirname(file_path)
    return os.path.basename(file_path)


def df_from_list_paths_files_excel(list_paths_files):
    print(f"{inspect.stack()[0].function} ... in processing")
    excel_dfs = []
    for file in list_paths_files:
        supplier = get_parent_folder_path(file, SUBFOLDERS.get('PARTNERS'))
        # check if the file name contains what you need
        if not file.startswith('.') and not file.startswith('~$'):
            print(supplier)
            df = pd.read_excel(file)
            df["Поставщик"] = supplier
            excel_dfs.append(df)
    return excel_dfs
