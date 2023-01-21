import flask
from app import app
import pandas as pd
from app.modules import decorators
import numpy as np
from random import randrange
from flask import flash, render_template


def df_from_list_paths_files_excel(list_paths_files):
    excel_dfs = []
    # use os.walk() to iterate through the subfolders - the first var
    for file in list_paths_files:
        # check if the file name contains what you need
        if not file.startswith('.') and not file.startswith('~$'):
            # read the Excel file into a DataFrame
            df = pd.read_excel(file)
            # add the DataFrame to the list
            excel_dfs.append(df)
    return excel_dfs
