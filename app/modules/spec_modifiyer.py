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
    """not working variant on 28.10.2020 with not one dimension of matrix"""
    replace_list = [False, 0, 0.0, 'Nan', np.nan, None, '', 'Null']
    df_merge = df1.merge(df2, how='outer', on=on, suffixes=('', '_drop'))
    df_merge[cols] = np.where(df1[cols].isin(replace_list), df2[cols], df1[cols]).astype(int)
    df_drop = df_merge.drop(columns=[x for x in df_merge.columns if '_drop' in x])

    return df_drop


def picking_prefixes(df, df_art_prefixes):
    """to fill df on coincidence startwith and in"""
    df['Префикс'] = ''
    df['Лекало'] = ''
    for art, idx in zip(df['Артикул товара'], range(len(df['Артикул товара']))):
        for pattern, idy in zip(df_art_prefixes["Лекало"], range(len(df_art_prefixes["Лекало"]))):
            for i in pattern.split():
                if f'-{i}-' in art and art.startswith(df_art_prefixes['Префикс'][idy]):
                    df['Лекало'][idx] = pattern
                    break
    return df


def picking_colors(df, df_colors):
    """colors picking from english"""
    idx = 0
    for art in df['Артикул товара']:
        jdx = 0
        for color in df_colors['Цвет английский']:
            if f'-{color.upper()}' in art:
                df['Цвет'][idx] = df_colors['Цвет русский'][jdx]
            jdx = jdx + 1
        idx = idx + 1
    return df


def df_selection(df_income, df_characters) -> pd.DataFrame:
    return df_income
