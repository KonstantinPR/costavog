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
from styleframe import StyleFrame, Styler, utils





@app.route('/get_wb_price_api', methods=['POST', 'GET'])
@login_required
def get_wb_price_api():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    df = detailing_reports.get_wb_price_api()
    file = io_output.io_output(df)
    return send_file(file, attachment_filename='price.xlsx', as_attachment=True)




@app.route('/revenue_processing', methods=['POST', 'GET'])
@login_required
def revenue_processing():
    """
    correcting existing discount via analise revenue dynamics and stocks
    """

    date_format = "%Y-%m-%d"
    DAYS_DELAY_REPORT = 5

    if not current_user.is_authenticated:
        return redirect('/company_register')

    if request.method == 'POST':

        # --- REQUEST PROCESSING ---
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

        # --- GET DATA VIA WB API /// ---
        df_sales = detailing_reports.get_wb_sales_realization_api(date_from, date_end, days_step)
        df_sales.to_excel('df_sales_excel_new.xlsx')
        # df_sales = pd.read_excel("wb_sales_report-2022-06-01-2022-06-30-00_00_00.xlsx")
        df_stock = detailing_reports.get_wb_stock_api()
        # df_stock = pd.read_excel("wb_stock.xlsx")

        # --- GET DATA FROM DB /// ---
        df_net_cost = pd.read_sql(
            db.session.query(Product).filter_by(company_id=app.config['CURRENT_COMPANY_ID']).statement, db.session.bind)

        df_sales_pivot = detailing_reports.get_wb_sales_realization_pivot(df_sales)
        # df_sales_pivot.to_excel('sales_pivot.xlsx')
        # таблица с итоговыми значениями с префиксом _sum
        df_sales_pivot.columns = [f'{x}_sum' for x in df_sales_pivot.columns]
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

        df_price = detailing_reports.get_wb_price_api()
        df = df_price.merge(df, how='outer', on='nm_id')

        df_complete = df_stock.merge(df, how='outer', on='nm_id')
        df = df_complete.merge(df_net_cost, how='left', left_on='sa_name', right_on='article')

        df = detailing_reports.get_revenue_by_part(df, period_dates_list)
        df = detailing_reports.df_stay_not_null(df)

        df = df.rename(columns={'Прибыль': f"Прибыль_{str(period_dates_list[0])[:10]}"})
        df_revenue_col_name_list = detailing_reports.df_revenue_col_name_list(df)

        # Формируем обобщающие показатели перед присоединением общей таблицы продаж с префиксом _sum
        df['Прибыль_max'] = df[df_revenue_col_name_list].max(axis=1)
        df['Прибыль_min'] = df[df_revenue_col_name_list].min(axis=1)
        df['Прибыль_sum'] = df[df_revenue_col_name_list].sum(axis=1)
        df['Прибыль_mean'] = df[df_revenue_col_name_list].mean(axis=1)
        df['Прибыль_first'] = df[df_revenue_col_name_list[0]]
        df['Прибыль_last'] = df[df_revenue_col_name_list[len(df_revenue_col_name_list) - 1]]
        df['Прибыль_growth'] = df['Прибыль_last'] - df['Прибыль_first']
        df['Логистика руб'] = df[[col for col in df.columns if "_rub_Логистика" in col]].sum(axis=1)
        df['Логистика шт'] = df[[col for col in df.columns if "_amount_Логистика" in col]].sum(axis=1)
        df['price_disc'] = df['price'] * (1 - df['discount'] / 100)

        # чтобы были видны итоговые значения из первоначальной таблицы с продажами
        df = df.merge(df_sales_pivot, how='outer', on='nm_id')

        df['Перечисление руб'] = df[[col for col in df.columns if "ppvz_for_pay_Продажа_sum" in col]].sum(axis=1) - \
                                 df[[col for col in df.columns if "ppvz_for_pay_Возврат_sum" in col]].sum(axis=1)

        # Принятие решения о скидке на основе сформированных данных ---
        # коэффициент влияния на скидку
        df['k_discount'] = 1
        # если не было продаж и текущая цена выше себестоимости, то увеличиваем скидку (коэффициент)
        df = detailing_reports.get_k_discount(df, df_revenue_col_name_list)
        df['Согласованная скидка, %'] = round(df['discount'] * df['k_discount'], 0)

        # df = detailing_reports.df_revenue_speed(df, period_dates_list)
        # реорганизуем порядок следования столбцов для лучшей читаемости
        df = detailing_reports.df_reorder_important_col_desc_first(df)
        df = detailing_reports.df_reorder_important_col_report_first(df)
        df = detailing_reports.df_reorder_revenue_col_first(df)
        df = df.sort_values(by='Прибыль_sum')

        # создаем стили для лучшей визуализации таблицы
        sf = StyleFrame(df)
        sf.apply_column_style(detailing_reports.IMPORTANT_COL_REPORT,
                              styler_obj=Styler(bg_color='FFFFCC'),
                              style_header=True)

        file = io_output.io_output_styleframe(sf)

        # добавляем полученный файл на яндекс.диск
        # is_added_to_yandex_disk =


        y.upload("file_to_upload.txt", "/destination.txt")

        return send_file(file,
                         attachment_filename=f"wb_revenue_report-{str(date_from)}-{str(date_end)}-{datetime.time()}.xlsx",
                         as_attachment=True)

    return render_template('upload_get_dynamic_sales.html')


@app.route('/get_wb_pivot_sells_api', methods=['POST', 'GET'])
@login_required
def get_wb_pivot_sells_api():
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
        df_stock = detailing_reports.get_wb_stock_api(date_from)
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
