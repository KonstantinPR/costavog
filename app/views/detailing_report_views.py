from app import app
from flask import render_template, request, redirect, send_file, flash
from flask_login import login_required, current_user
from app.models import Product, db
import datetime
import pandas as pd
from app.modules import detailing, detailing_reports, yandex_disk_handler
from app.modules import io_output
import time
import numpy as np


def revenue_per_one(rev, sel, net, log):
    rev_per_one = 0
    if sel and rev:
        return int(rev / sel)
    if log:
        return net - log
    return rev_per_one


def revenue_net_dif(rev_per, net):
    rev_net_dif = 1
    if rev_per and net:
        rev_net_dif = rev_per / net
    return rev_net_dif


def revenue_potential_cost(rev_per, net, qt, k_dif):
    rev_pot = net * k_dif * qt
    if rev_per:
        return rev_per * qt
    return rev_pot


@app.route('/key_indicators', methods=['POST', 'GET'])
@login_required
def key_indicators():
    """
    to show key indicators from revenue_tables via yandex_disk file (or revenue_processing route - planning in future)
    1 . market cost of all products on wb
    2 . revenue potential cost of all product on wb (to take medium of revenue if no sells)
    (potential cost by revenue of all products)
    """

    if not current_user.is_authenticated:
        return redirect('/company_register')
    key_indicators = {}
    file_content, file_name = yandex_disk_handler.download_from_yandex_disk()
    df = file_content

    df['market_cost'] = df['price_disc'] * df['quantityFull']
    key_indicators['market_cost'] = df['market_cost'].sum()
    key_indicators['net_cost_med'] = (df[df["net_cost"] != 0]["net_cost"] * df[df["net_cost"] != 0][
        "quantityFull"]).sum() / df[df["net_cost"] != 0]["quantityFull"].sum()

    df['net_cost'] = np.where(df.net_cost == 0, key_indicators['net_cost_med'], df.net_cost)

    df['nets_cost'] = df['net_cost'] * df['quantityFull']
    key_indicators['nets_cost'] = df['nets_cost'].sum()
    df['sells_qt_with_back'] = df['quantity_Продажа_sum'] - df['quantity_Возврат_sum']
    key_indicators['sells_qt_with_back'] = df['sells_qt_with_back'].sum()
    df['revenue_per_one'] = [revenue_per_one(rev, sel, net, log) for
                             rev, sel, net, log in
                             zip(df['Прибыль_sum'],
                                 df['sells_qt_with_back'],
                                 df['net_cost'],
                                 df['Логистика руб'],
                                 )]
    df['revenue_net_dif'] = [revenue_net_dif(rev_per, net) for
                             rev_per, net in
                             zip(df['revenue_per_one'],
                                 df['net_cost'],
                                 )]

    key_indicators['revenue_net_dif_med'] = df[df["revenue_net_dif"] != 1]["revenue_net_dif"].mean()

    df['revenue_potential_cost'] = [
        revenue_potential_cost(rev_per, net, qt, k_dif=key_indicators['revenue_net_dif_med']) for
        rev_per, net, qt in
        zip(df['revenue_per_one'],
            df['net_cost'],
            df['quantityFull'],
            )]

    key_indicators['revenue_potential_cost'] = df['revenue_potential_cost'].sum()

    for k, v in key_indicators.items():
        if not 'revenue_net_dif_med' in k:
            key_indicators[k] = int(v)
        print(f'{k} {key_indicators[k]}')

    df = df.from_dict(key_indicators, orient='index', columns=['key_indicator'])
    file = io_output.io_output(df)

    return send_file(file, download_name=f'key_indicator_of_{file_name}', as_attachment=True)


@app.route('/revenue_processing', methods=['POST', 'GET'])
@login_required
def revenue_processing():
    """
    correcting existing discount via analise revenue dynamics and stocks
    """

    if not current_user.is_authenticated:
        return redirect('/company_register')

    if request.method == 'POST':
        df, file_name = detailing_reports.revenue_processing_module(request)
        print(file_name)
        file_excel = io_output.io_output(df)
        return send_file(file_excel, download_name=file_name, as_attachment=True)

    return render_template('upload_get_dynamic_sales.html')


@app.route('/get_wb_price_api', methods=['POST', 'GET'])
@login_required
def get_wb_price_api():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    df = detailing_reports.get_wb_price_api()
    file_content = io_output.io_output(df)
    return send_file(file_content, attachment_filename='price.xlsx', as_attachment=True)


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
        df = detailing_reports.get_revenue(df)
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
