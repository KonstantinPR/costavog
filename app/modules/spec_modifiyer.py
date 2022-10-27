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


def verticalization_sizes(df):
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


def picking_characters(df_income, df_spec_example) -> pd.DataFrame:
    df_spec = df_spec_example.merge(df_income, how='outer', on='Артикул товара', suffixes=("_drop_column_on", ""))
    df_spec.drop([col for col in df_spec.columns if '_drop_column_on' in col], axis=1, inplace=True)

    return df_spec


def df_selection(df_income, df_characters) -> pd.DataFrame:
    for i in df_characters['Лекало']:
        pass
    return df_income
