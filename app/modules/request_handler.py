import datetime
import os
import logging

import flask

from app import app
import pandas as pd
from flask import flash
import json
from werkzeug.datastructures import FileStorage

DATE_FORMAT = "%Y-%m-%d"


def gather_request_data(request: flask.Request) -> dict:
    # Retrieve the checkbox names from the hidden input
    checkbox_names = request.form.get('checkbox_names', '[]')
    checkbox_names = json.loads(checkbox_names)

    req_d = {}

    # Collect values from checkboxes
    for checkbox in checkbox_names:
        req_d[checkbox] = checkbox in request.form

    # Collect other form inputs
    for key in request.form:
        if key != 'checkbox_names':  # Skip the hidden input
            req_d[key] = request.form[key]  # Collect value for other inputs

    return req_d


def get_files(request):
    # Check if the post request has the file part
    if 'file' not in request.files:
        return 'No file part', 400

    files = request.files.getlist("file")

    # Ensure at least one file is uploaded
    if len(files) == 0:
        return 'No files uploaded', 400
    return files


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

    if df.empty: flash("Необходимые данные не переданы")
    return df


# def to_df(request, html_text_input_name='text_input', html_file_input_name='file', input_column='vendorCode'):
#     """To get request and take from it text_input and make from it df, or take file and make df"""
#     df = pd.DataFrame
#
#     if request.form[html_text_input_name]:
#         print(html_file_input_name)
#         input_text = request.form[html_text_input_name]
#         input_text = input_text.split(" ")
#         df = pd.DataFrame(input_text, columns=[input_column])
#         return df
#     elif request.files[html_file_input_name]:
#         input_txt = request.files[html_file_input_name]
#         filename = input_txt.filename
#         try:
#             df = pd.read_csv(input_txt, sep='	', names=[input_column])
#             df = pd.read_csv(input_txt, sep='	')
#             if df[input_column][0] == input_column: df = df.drop([0, 0]).reset_index(drop=True)
#         except:
#             df = pd.read_excel(input_txt)
#         return df
#     flash("Необходимые данные не переданы")
#     return df


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
    print(f"is_checkbox_true...")
    if request:
        print(f"request passed")
        if request_name in request.form:
            print(f"request_name in request.form")
            return True
    return False


def request_date_from(request, date_format=DATE_FORMAT, delta=app.config['DAYS_STEP_DEFAULT']):
    if request.form.get('date_from'):
        date_from = request.form.get('date_from')
    else:
        date_from = datetime.datetime.today() - datetime.timedelta(
            days=delta) - datetime.timedelta(app.config['DAYS_DELAY_REPORT'])
        date_from = date_from.strftime(date_format)
    return date_from


def request_date_end(request, date_format=DATE_FORMAT):
    if request.form.get('date_end'):
        date_end = request.form.get('date_end')
    else:
        date_end = datetime.datetime.today() - datetime.timedelta(app.config['DAYS_DELAY_REPORT'])
        date_end = date_end.strftime(date_format)
        # date_end = time.strftime(date_format)- datetime.timedelta(3)
    return date_end


def request_days_step(request):
    if request.form.get('days_step'):
        days_step = request.form.get('days_step')
    else:
        days_step = app.config['DAYS_STEP_DEFAULT']
    return days_step


def date_handler(request, date_from, date_end, format_date):
    if request:
        if not date_from:
            date_from = request_date_from(request)
            print(f"date_from {date_from} created from request ...")
        if not date_end:
            date_end = request_date_end(request)
            print(f"date_end {date_end} created from request ...")

    # Convert the input date strings to datetime objects
    date_from = datetime.datetime.strptime(date_from, format_date).strftime(format_date)
    date_end = datetime.datetime.strptime(date_end, format_date).strftime(format_date)
    return date_from, date_end
