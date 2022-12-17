import app.modules.API_WB
from app import app
from flask import render_template, request, redirect, send_file
from flask_login import login_required, current_user
import datetime
from app.modules import detailing, detailing_reports, yandex_disk_handler, pandas_handler
from app.modules import io_output
import time


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

    file_content, file_name = yandex_disk_handler.download_from_yandex_disk()
    df = detailing_reports.key_indicators_module(file_content)

    file_name_key_indicator = f'key_indicator_of_{file_name}'
    file_content = io_output.io_output(df, is_index=True)
    yandex_disk_handler.upload_to_yandex_disk(file_content, file_name_key_indicator)

    file = io_output.io_output(df)

    return send_file(file, download_name=file_name_key_indicator, as_attachment=True)


@app.route('/revenue_processing', methods=['POST', 'GET'])
@login_required
def revenue_processing():
    """
    report on any periods of date
    """

    if not current_user.is_authenticated:
        return redirect('/company_register')

    if request.method == 'POST':
        df, file_name = detailing_reports.revenue_processing_module(request)
        print(file_name)
        file_excel = io_output.io_output(df)
        return send_file(file_excel, download_name=file_name, as_attachment=True)

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
        df_sales_wb_api = app.modules.API_WB.get_wb_sales_realization_api(date_from, date_end, days_step)
        print(time.process_time() - t)
        file = io_output.io_output(df_sales_wb_api)
        print(time.process_time() - t)
        return send_file(file,
                         attachment_filename=f"wb_sales_report-{str(date_from)}-{str(date_end)}-{datetime.time()}.xlsx",
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

        df = app.modules.API_WB.get_wb_sales_realization_api(date_from, date_end, days_step)
        df_sales = detailing_reports.get_wb_sales_realization_pivot(df)
        df_stock = app.modules.API_WB.get_wb_stock_api(date_from)
        df_net_cost = yandex_disk_handler.get_excel_file_from_ydisk(app.config['NET_COST_PRODUCTS'])
        df = df_sales.merge(df_stock, how='outer', on='nm_id')
        df = df.merge(df_net_cost, how='outer', left_on='nm_id', right_on='nm_id')
        df = detailing_reports.get_revenue(df)
        df = detailing_reports.get_important_columns(df)
        file = io_output.io_output(df)
        name_of_file = f"wb_revenue_report-{str(date_from)}-{str(date_end)}-{datetime.time()}.xlsx"
        return send_file(file, attachment_filename=name_of_file, as_attachment=True)

    return render_template('upload_get_dynamic_sales.html')


@app.route('/get_wb_price_api', methods=['POST', 'GET'])
@login_required
def get_wb_price_api():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    df = detailing_reports.get_wb_price_api()
    file_content = io_output.io_output(df)
    return send_file(file_content, attachment_filename='price.xlsx', as_attachment=True)


# @app.route('/get_wb_stock', methods=['POST', 'GET'])
# @login_required
# def get_wb_stock():
#     if not current_user.is_authenticated:
#         return redirect('/company_register')
#
#     df = detailing.get_wb_stock()
#     file = io_output.io_output(df)
#
#     return send_file(file, attachment_filename='report' + str(datetime.date.today()) + ".xlsx", as_attachment=True)


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

        # if request.form.get('days_step'):
        #     days_step = request.form.get('days_step')
        # else:
        #     days_step = app.config['DAYS_STEP_DEFAULT']

        t = time.process_time()
        print(time.process_time() - t)
        # df_sales_wb_api = detailing.get_wb_sales_api(date_from, days_step)
        # df_sales_wb_api = detailing.get_wb_sales_realization_api(date_from, date_end, days_step)
        df_sales_wb_api = app.modules.API_WB.get_wb_stock_api()
        print(time.process_time() - t)
        file = io_output.io_output(df_sales_wb_api)
        print(time.process_time() - t)
        return send_file(file,
                         attachment_filename='report' + str(datetime.date.today()) + str(datetime.time()) + ".xlsx",
                         as_attachment=True)

    return render_template('upload_get_dynamic_sales.html')
