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
from yadisk import YaDisk


def get_list_paths_files(path_to_files_glob: str,
                         file_names: list[str] = [app.config['ARRIVAL_FILE_NAMES']],
                         file_extension: str = app.config['EXTENSION_EXCEL'],
                         ):
    print(f"{inspect.stack()[0].function} ... in processing")
    list_paths_files = []
    for file_name in file_names:
        path_files = f"{path_to_files_glob}{file_name}{file_extension}"
        print(f"path_files {path_files}")
        list_paths_files.extend(glob.glob(path_files, recursive=True))
        print(f"list_paths_files {list_paths_files}")
    return list_paths_files


def df_from_list_paths_excel_files(list_paths_files, col_name_from_path=True):
    print(f"{inspect.stack()[0].function} ... in processing")
    excel_dfs = []
    for file in list_paths_files:
        # check if the file name contains what you need
        if not file.startswith('.') and not file.startswith('~$'):
            df = pd.read_excel(file)
            if col_name_from_path:
                df = add_col_from_path_to_df(df, file)
            excel_dfs.append(df)
    return excel_dfs


def add_col_from_path_to_df(df, file_path):
    print(file_path)
    file_path_split = file_path.split("\\")
    len_file_path_list = len(file_path_split)
    df["file_path"] = file_path
    for i in range(len_file_path_list):
        df[f"Column {i}"] = file_path_split[i]

    return df
