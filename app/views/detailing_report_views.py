from app import app
import flask
import requests
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required, current_user, login_user, logout_user
from app.models import Company, UserModel, Transaction, Task, Product, db
import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import desc
import pandas as pd
from io import BytesIO
import numpy as np
from sqlalchemy import create_engine
from urllib.parse import urlencode
from app.modules import discount, detailing, detailing_reports, sql_query_main
from app.modules import io_output
import time


@app.route('/get_speed_revenue', methods=['POST', 'GET'])
@login_required
def get_speed_revenue():
    """

    to get some periods of sells and take speed data on revenue
    for examples first period revenue 5, second -2, third 1:
    1. gap between max and min revenue in all periods
    so can count medium: 5+(-2)+1 = 4
    or count speed: first: -2-5 = -7, second: 1-(-2) = 3, speed = (-7 + 3) / 2 = -2
    or count max speed: 3, min speed: -7

    """

    date_format = "%Y-%m-%d"
    DAYS_DELAY_REPORT = 5

    if not current_user.is_authenticated:
        return redirect('/company_register')

    if request.method == 'POST':

        if request.form.get('date_from'):
            date_from = request.form.get('date_from')
        else:
            date_from = datetime.datetime.today() - datetime.timedelta(
                days=app.config['DAYS_STEP_DEFAULT']) - datetime.timedelta(DAYS_DELAY_REPORT)
            date_from = date_from.strftime(date_format)

        print(f"type is {type(date_from)}")

        if request.form.get('date_end'):
            date_end = request.form.get('date_end')
        else:
            date_end = datetime.datetime.today() - datetime.timedelta(DAYS_DELAY_REPORT)
            date_end = date_end.strftime(date_format)
            # date_end = time.strftime(date_format)- datetime.timedelta(3)

        print(date_end)

        if request.form.get('days_step'):
            days_step = request.form.get('days_step')
        else:
            days_step = app.config['DAYS_STEP_DEFAULT']

        if request.form.get('part_by'):
            date_parts = request.form.get('part_by')
        else:
            date_parts = 3

        # df_sales = detailing_reports.get_wb_sales_realization_api(date_from, date_end, days_step)
        df_sales = pd.read_excel("wb_sales_report-2022-06-01-2022-06-30-00_00_00.xlsx")

        days_bunch = detailing_reports.get_days_bunch_from_delta_date(date_from, date_end, date_parts, date_format)
        period_dates_list = detailing_reports.get_period_dates_list(date_from, date_end, days_bunch, date_parts)
        df_sales_list = detailing_reports.dataframe_divide(df_sales, period_dates_list, date_from)

        # df_pivot_list = []
        df_pivot_list = [detailing_reports.get_wb_sales_realization_pivot(d) for d in df_sales_list]

        df = df_pivot_list[0]
        date = iter(period_dates_list[1:])
        df_p = df_pivot_list[1:]
        for d in df_p:
            df_pivot = df.merge(d, how="left", on='nm_id', suffixes=(None, f'_{str(next(date))[:10]}'))
            df = df_pivot

        # df_stock = detailing_reports.get_wb_stock_api()
        df_stock = pd.read_excel("wb_stock.xlsx")
        df_complete = df_stock.merge(df, how='outer', on='nm_id')

        df_net_cost = pd.read_sql(
            db.session.query(Product).filter_by(company_id=app.config['CURRENT_COMPANY_ID']).statement, db.session.bind)
        df = df_complete.merge(df_net_cost, how='left', left_on='sa_name', right_on='article')

        df = detailing_reports.get_revenue_column_by_part(df, period_dates_list)
        df = detailing_reports.df_stay_not_null(df)

        df = df.rename(columns={'Прибыль': f"Прибыль_{str(period_dates_list[0])[:10]}"})
        df_revenue_col_name_list = detailing_reports.df_revenue_col_name_list(df)
        df['Прибыль_max'] = df[df_revenue_col_name_list].max(axis=1)
        df['Прибыль_min'] = df[df_revenue_col_name_list].min(axis=1)
        df['Прибыль_sum'] = df[df_revenue_col_name_list].sum(axis=1)
        df['Прибыль_mean'] = df[df_revenue_col_name_list].mean(axis=1)
        df['Прибыль_growth %'] = df[df_revenue_col_name_list[2]] - df[df_revenue_col_name_list[1]]
        df['Логистика руб'] = df[[col for col in df.columns if "_rub_Логистика" in col]].sum(axis=1)

        # df = detailing_reports.df_revenue_speed(df, period_dates_list)
        df = detailing_reports.change_order_df_columns(df)
        df = detailing_reports.df_reorder_important_col_first(df)

        file = io_output.io_output(df)

        return send_file(file,
                         attachment_filename=f"wb_revenue_report-{str(date_from)}-{str(date_end)}-{datetime.time()}.xlsx",
                         as_attachment=True)

    return render_template('upload_get_dynamic_sales.html')


@app.route('/get_wb_pivot_sells_api', methods=['POST', 'GET'])
@login_required
def get_wb_pivot_sells_api():
    """To get speed of sales for all products in period"""
    if not current_user.is_authenticated:
        return redirect('/company_register')
    if request.method == 'POST':

        if request.form.get('date_from'):
            date_from = request.form.get('date_from')
        else:
            date_from = datetime.datetime.today() - datetime.timedelta(days=app.config['DAYS_STEP_DEFAULT'])
            date_from = date_from.strftime("%Y-%m-%d")

        print(date_from)

        if request.form.get('date_end'):
            date_end = request.form.get('date_end')
        else:
            date_end = time.strftime("%Y-%m-%d")

        print(date_end)

        if request.form.get('days_step'):
            days_step = request.form.get('days_step')
        else:
            days_step = app.config['DAYS_STEP_DEFAULT']

        df = detailing_reports.get_wb_sales_realization_api(date_from, date_end, days_step)
        df_sales = detailing_reports.get_wb_sales_realization_pivot(df)
        df_stock = detailing_reports.get_wb_stock_api(date_from, date_end, days_step)
        df_net_cost = pd.read_sql(
            db.session.query(Product).filter_by(company_id=app.config['CURRENT_COMPANY_ID']).statement, db.session.bind)
        df = df_sales.merge(df_stock, how='outer', on='nm_id')
        df = df.merge(df_net_cost, how='outer', left_on='supplierArticle', right_on='article')
        df = detailing_reports.get_revenue_column(df)
        df = detailing_reports.get_important_columns(df)
        file = io_output.io_output(df)

        return send_file(file,
                         attachment_filename=f"wb_revenue_report-{str(date_from)}-{str(date_end)}-{datetime.time()}.xlsx",
                         as_attachment=True)

    return render_template('upload_get_dynamic_sales.html')


@app.route('/get_wb_sales_realization_api', methods=['POST', 'GET'])
@login_required
def get_wb_sales_realization_api():
    """To get speed of sales for all products in period"""
    if not current_user.is_authenticated:
        return redirect('/company_register')
    if request.method == 'POST':
        if request.form.get('date_from'):
            date_from = request.form.get('date_from')
        else:
            date_from = datetime.datetime.today() - datetime.timedelta(days=app.config['DAYS_STEP_DEFAULT'])
            date_from = date_from.strftime("%Y-%m-%d")

        print(date_from)

        if request.form.get('date_end'):
            date_end = request.form.get('date_end')
        else:
            date_end = time.strftime("%Y-%m-%d")

        print(date_end)

        if request.form.get('days_step'):
            days_step = request.form.get('days_step')
        else:
            days_step = app.config['DAYS_STEP_DEFAULT']
        t = time.process_time()
        print(time.process_time() - t)
        # df_sales_wb_api = detailing.get_wb_sales_api(date_from, days_step)
        # df_sales_wb_api = detailing.get_wb_sales_realization_api(date_from, date_end, days_step)
        df_sales_wb_api = detailing_reports.get_wb_sales_realization_api(date_from, date_end, days_step)
        print(time.process_time() - t)
        file = io_output.io_output(df_sales_wb_api)
        print(time.process_time() - t)
        return send_file(file,
                         attachment_filename=f"wb_sales_report-{str(date_from)}-{str(date_end)}-{datetime.time()}.xlsx",
                         as_attachment=True)

    return render_template('upload_get_dynamic_sales.html')


@app.route('/get_wb_stock', methods=['POST', 'GET'])
@login_required
def get_wb_stock():
    if not current_user.is_authenticated:
        return redirect('/company_register')

    df = detailing.get_wb_stock()
    file = io_output.io_output(df)

    return send_file(file, attachment_filename='report' + str(datetime.date.today()) + ".xlsx", as_attachment=True)


@app.route('/get_wb_stock_api', methods=['POST', 'GET'])
@login_required
def get_wb_stock_api():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    if request.method == 'POST':
        if request.form.get('date_from'):
            date_from = request.form.get('date_from')
        else:
            date_from = datetime.datetime.today() - datetime.timedelta(days=app.config['DAYS_STEP_DEFAULT'])
            date_from = date_from.strftime("%Y-%m-%d")

        print(date_from)

        if request.form.get('date_end'):
            date_end = request.form.get('date_end')
        else:
            date_end = time.strftime("%Y-%m-%d")

        print(date_end)

        if request.form.get('days_step'):
            days_step = request.form.get('days_step')
        else:
            days_step = app.config['DAYS_STEP_DEFAULT']
        t = time.process_time()
        print(time.process_time() - t)
        # df_sales_wb_api = detailing.get_wb_sales_api(date_from, days_step)
        # df_sales_wb_api = detailing.get_wb_sales_realization_api(date_from, date_end, days_step)
        df_sales_wb_api = detailing_reports.get_wb_stock_api()
        print(time.process_time() - t)
        file = io_output.io_output(df_sales_wb_api)
        print(time.process_time() - t)
        return send_file(file,
                         attachment_filename='report' + str(datetime.date.today()) + str(datetime.time()) + ".xlsx",
                         as_attachment=True)

    return render_template('upload_get_dynamic_sales.html')
