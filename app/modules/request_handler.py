import os
import logging
from app import app
import pandas as pd
from flask import flash
from werkzeug.datastructures import FileStorage


def to_df(request, html_text_input_name='text_input', html_file_input_name='file', input_column='vendorCode'):
    """To get request and take from it text_input and make from it df, or take file and make df"""
    df = pd.DataFrame

    if request.form[html_text_input_name]:
        print(html_file_input_name)
        input_text = request.form[html_text_input_name]
        input_text = input_text.split(" ")
        df = pd.DataFrame(input_text, columns=[input_column])
        return df
    elif request.files[html_file_input_name]:
        input_txt = request.files[html_file_input_name]
        filename = input_txt.filename
        try:
            df = pd.read_csv(input_txt, sep='	', names=[input_column])
            if df[input_column][0] == input_column: df = df.drop([0, 0]).reset_index(drop=True)
        except:
            df = pd.read_excel(input_txt)
        return df
    flash("Необходимые данные не переданы")
    return df


def file_name_from_request(request, html_file_input_name='file'):
    if request.files[html_file_input_name]:
        input_txt = request.files[html_file_input_name]
        filename = input_txt.filename
        return filename
    return "output_file"


def request_last_days(request, input_name, config_name_default='DAYS_STEP_DEFAULT'):
    if request.form.get(input_name):
        days = request.form.get(input_name)
    else:
        days = app.config['LAST_DAYS_DEFAULT']
    return days


def is_checkbox_true(request=None, request_name=None):
    if request and request_name in request.form:
        return True
    return False

