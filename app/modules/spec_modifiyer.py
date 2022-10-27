from app import app
from flask import render_template, request, redirect, send_file
from flask_login import login_required, current_user
from app.models import Product, db
import datetime
import pandas as pd
from app.modules import detailing, detailing_reports, yandex_disk_handler, decorators
from app.modules import io_output
import time
import flask
from werkzeug.datastructures import FileStorage
from io import BytesIO
from app import app
from flask import flash, render_template, request, redirect, send_file
from app.modules import text_handler, io_output
import numpy as np
from flask_login import login_required, current_user, login_user, logout_user
from dataclasses import dataclass


@decorators.flask_request_to_df
def request_to_df(flask_request) -> pd.DataFrame:
    df = flask_request
    return df


def vertical_size(df):
    df = df.assign(sizes=df['Размеры'].str.split()).explode('sizes')
    df = df.drop(['Размеры'], axis=1)
    df = df.rename({'sizes': 'Размер'}, axis='columns')
    print(df.columns)

    # OLD RIGHT
    # lst_art = []
    # lst_sizes = [x.split() for x in df['Размеры']]
    # for i in range(len(lst_sizes)):
    #     for j in range(len(lst_sizes[i])):
    #         lst_art.append(df['Артикул полный'][i])
    #
    # lst_sizes = sum(lst_sizes, [])
    # df = pd.DataFrame({'Артикул полный': lst_art, 'Размеры': lst_sizes})

    print(df)

    return df


def merge_spec(df_income, df_spec_example, on) -> pd.DataFrame:
    df_spec = df_spec_example.merge(df_income, how='outer', on=on, suffixes=("_drop_column_on", ""))
    df_spec.drop([col for col in df_spec.columns if '_drop_column_on' in col], axis=1, inplace=True)
    df_spec.dropna(how='all', axis=1, inplace=True)
    df_spec = df_spec[df_spec[on].notna()]

    return df_spec


# def merge_dataframes2(df_income, df_spec_example, on) -> pd.DataFrame:
#     df_spec = df_spec_example.merge(df_income, how='outer', on=on)
#     # df_spec.drop([col for col in df_spec.columns if '_drop_column_on' in col], axis=1, inplace=True)
#
#     return df_spec


def merge_nan_drop(df1, df2, on, cols):
    replace_list = [False, 0, 0.0, 'Nan', np.nan, None, '', 'Null']
    df_merge = df1.merge(df2, how='outer', on=on, suffixes=('', '_drop'))
    df_merge[cols] = np.where(df1[cols].isin(replace_list), df2[cols], df1[cols]).astype(int)
    df_drop = df_merge.drop(columns=[x for x in df_merge.columns if '_drop' in x])

    return df_drop


def picking_prefixes(df, df_art_prefixes):
    """search what kind of product in spec via name of art, for example if begin SK and contain -B- then is eco-fur"""
    prefixes = set([x for x in df_art_prefixes['Префикс']])
    df['Префикс'] = ''
    idx = 0
    for art in df['Артикул товара']:
        for pre in prefixes:
            if art.startswith(pre):
                df['Префикс'][idx] = pre
            if f'-{pre}-' in art:
                df['Префикс'][idx] = pre
            if ' ' in pre:
                print('YEA DETKS')
                pre_lst = pre.split()
                print(f'pre_list is {pre_lst}')
                for i in pre_lst:
                    print(f'i is {i} in {art}')
                    if i in art:
                        df['Префикс'][idx] = pre

        idx = idx + 1

    print(prefixes)
    return df


def df_selection(df_income, df_characters) -> pd.DataFrame:
    return df_income
