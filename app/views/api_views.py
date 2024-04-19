import logging
from app import app
from flask import render_template, request, redirect, send_file
from flask_login import login_required, current_user
import datetime
import time
from app.modules import API_WB, detailing_api_module
from app.modules import io_output, yandex_disk_handler, request_handler, pandas_handler


@app.route('/get_sales_funnel_wb', methods=['POST', 'GET'])
@login_required
def get_sales_funnel_wb():
    """
    To get sales_funnel via API WB
    """

    if request.method == 'POST':
        date_from = detailing_api_module.request_date_from(request)
        date_end = detailing_api_module.request_date_end(request)
        is_from_yadisk = request.form.get('is_from_yadisk')
        testing_mode = request.form.get('testing_mode')
        is_erase_points = request.form.get('is_erase_points')
        is_exclude_nmIDs = request.form.get('is_exclude_nmIDs')

        # Retrieve nmIDs from API and exclude cards from Yandex Disk
        df_nmIDs = API_WB.get_all_cards_api_wb(testing_mode=testing_mode, is_from_yadisk=is_from_yadisk)
        nmIDs = df_nmIDs['nmID'].unique()
        if is_exclude_nmIDs:
            nmIDs_exclude = yandex_disk_handler.download_from_YandexDisk(path='YANDEX_EXCLUDE_CARDS')[0]['nmID']
            nmIDs = pandas_handler.nmIDs_exclude(nmIDs, nmIDs_exclude)

        # Retrieve sales funnel data frame
        df = API_WB.get_wb_sales_funnel_api(nmIDs, date_from, date_end, is_erase_points=is_erase_points)

        # Log a message indicating successful retrieval of sales funnel data
        logging.info("Sales funnel data retrieved successfully")

        # Process and output the data frame
        df = io_output.io_output(df)
        file_name = f'wb_sales_funnel_{str(date_from)}_{str(date_end)}.xlsx'
        return send_file(df, download_name=file_name, as_attachment=True)

    # If the request method is not POST, render the upload_get_sales_funnel.html template
    return render_template('upload_get_sales_funnel.html', doc_string=get_cards_wb.__doc__)


@app.route('/get_sales_wb', methods=['POST', 'GET'])
@login_required
def get_sales_wb():
    """
    To get sales via API WB
    """

    if request.method == 'POST':
        date_from = detailing_api_module.request_date_from(request)
        date_end = detailing_api_module.request_date_end(request)
        days_step = detailing_api_module.request_days_step(request)
        df_sales = API_WB.get_wb_sales_realization_api_v2(date_from, date_end, days_step)
        df = io_output.io_output(df_sales)
        file_name = f'wb_sales_{str(date_from)}_{str(date_end)}.xlsx'
        return send_file(df, download_name=file_name, as_attachment=True)
    return render_template('upload_sales_wb.html', doc_string=get_cards_wb.__doc__)


@app.route('/get_cards_wb', methods=['POST', 'GET'])
@login_required
def get_cards_wb():
    """
    To get all cards via API WB
    """

    if request.method == 'POST':
        is_from_yadisk = request.form.get('is_from_yadisk')
        df_all_cards = API_WB.get_all_cards_api_wb(is_from_yadisk=is_from_yadisk)
        df = io_output.io_output(df_all_cards)
        file_name = f'wb_api_cards_{str(datetime.datetime.now())}.xlsx'
        return send_file(df, download_name=file_name, as_attachment=True)
    return render_template('upload_cards_wb.html', doc_string=get_cards_wb.__doc__)


@app.route('/get_stock_wb', methods=['POST', 'GET'])
@login_required
def get_stock_wb():
    """
    Достает все остатки с WB через API
    """
    is_delete_shushary = request.form.get('is_delete_shushary')
    logging.info(f"is_delete_shushary {is_delete_shushary}")
    file_name = f'wb_api_stock_{str(datetime.datetime.now())}.xlsx'
    if request.method == 'POST':
        logging.info(f"request {request}")
        df = API_WB.get_wb_stock_api(request=request, is_delete_shushary=is_delete_shushary)
        df = io_output.io_output(df)
        return send_file(df, download_name=file_name, as_attachment=True)
    return render_template('upload_stock_wb.html', doc_string=get_stock_wb.__doc__)


@app.route('/get_storage_wb', methods=['POST', 'GET'])
@login_required
def get_storage_wb():
    """
    Get all storage cost for goods between two data, default last week
    """

    if request.method == 'POST':
        number_last_days = request_handler.request_last_days(request, input_name='number_last_days')
        if not number_last_days: number_last_days = app.config['LAST_DAYS_DEFAULT']
        logging.info(f'number_last_days {number_last_days}')
        logging.info(f"{request.form.get('is_mean')}")
        if request.form.get('is_mean'):
            df_all_cards = API_WB.get_average_storage_cost()
            logging.info(f"storage cost is received by API WB")
        else:
            df_all_cards = API_WB.get_storage_cost(number_last_days, days_delay=0)

        # logging.info(f"df {df_all_cards}")
        df = io_output.io_output(df_all_cards)
        file_name = f'storage_data_{str(datetime.datetime.now())}.xlsx'
        return send_file(df, download_name=file_name, as_attachment=True)
    return render_template('upload_storage_wb.html', doc_string=get_storage_wb.__doc__)


@app.route('/get_wb_sales_realization_api', methods=['POST', 'GET'])
@login_required
def get_wb_sales_realization_api():
    """To get speed of sales for all products in period"""
    if not current_user.is_authenticated:
        return redirect('/company_register')
    if request.method == 'POST':
        date_from = detailing_api_module.request_date_from(request)
        date_end = detailing_api_module.request_date_end(request)
        days_step = detailing_api_module.request_days_step(request)
        t = time.process_time()
        logging.info(time.process_time() - t)
        # df_sales_wb_api = detailing.get_wb_sales_api(date_from, days_step)
        # df_sales_wb_api = detailing.get_wb_sales_realization_api(date_from, date_end, days_step)
        df_sales_wb_api = API_WB.get_wb_sales_realization_api(date_from, date_end, days_step)
        logging.info(time.process_time() - t)
        file = io_output.io_output(df_sales_wb_api)
        logging.info(time.process_time() - t)
        return send_file(file, download_name=f"wb_sales_report-{str(date_from)}-{str(date_end)}-{datetime.time()}.xlsx",
                         as_attachment=True)

    return render_template('upload_get_dynamic_sales.html')


@app.route('/get_wb_pivot_sells_api', methods=['POST', 'GET'])
@login_required
def get_wb_pivot_sells_api() -> object:
    """
    Форма возвращает отчет в excel о прибыльности на основе анализа отчетов о продажах, остатков с сайта wb
    и данных о себестоимости с яндексдиска. Отчет формируется От и до указанных дат - в случае с "динамикой"
    отчет будет делиться на указанное количество частей, в каждой из которых будет высчитываться прибыль и
    далее рассчитыватья показатели на основе изменения прибыли от одного периода к другому. На выходе
    получим сводную таблицу с реккомендациями по скидке.
    """
    if not current_user.is_authenticated:
        return redirect('/company_register')
    if request.method == 'POST':
        date_from = detailing_api_module.request_date_from(request)
        date_end = detailing_api_module.request_date_end(request)
        days_step = detailing_api_module.request_days_step(request)
        df = API_WB.get_wb_sales_realization_api(date_from, date_end, days_step)
        df_sales = detailing_api_module.get_wb_sales_realization_pivot(df)
        df_stock = API_WB.get_wb_pivot_sells_api()
        df_net_cost = yandex_disk_handler.get_excel_file_from_ydisk(app.config['NET_COST_PRODUCTS'])
        df = df_sales.merge(df_stock, how='outer', on='nm_id')
        df = df.merge(df_net_cost, how='outer', left_on='nm_id', right_on='nm_id')
        df = detailing_api_module.get_revenue(df)
        df = detailing_api_module.get_important_columns(df)
        file = io_output.io_output(df)
        name_of_file = f"wb_revenue_report-{str(date_from)}-{str(date_end)}-{datetime.time()}.xlsx"
        return send_file(file, download_name=name_of_file, as_attachment=True)

    return render_template('upload_get_dynamic_sales.html', doc_string=get_wb_pivot_sells_api.__doc__)


@app.route('/get_wb_price_api', methods=['POST', 'GET'])
@login_required
def get_wb_price_api():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    df = detailing_api_module.get_wb_price_api()
    file_content = io_output.io_output(df)
    return send_file(file_content, download_name='price.xlsx', as_attachment=True)


# @app.route('/get_wb_stock', methods=['POST', 'GET'])
# @login_required
# def get_wb_stock():
#     if not current_user.is_authenticated:
#         return redirect('/company_register')
#
#     df = detailing.get_wb_stock()
#     file = io_output.io_output(df)
#
#     return send_file(file, download_name='report' + str(datetime.date.today()) + ".xlsx", as_attachment=True)


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

        logging.info(date_from)

        if request.form.get('date_end'):
            date_end = request.form.get('date_end')
        else:
            date_end = time.strftime("%Y-%m-%d")

        logging.info(date_end)

        # if request.form.get('days_step'):
        #     days_step = request.form.get('days_step')
        # else:
        #     days_step = app.config['DAYS_STEP_DEFAULT']

        t = time.process_time()
        logging.info(time.process_time() - t)
        # df_sales_wb_api = detailing.get_wb_sales_api(date_from, days_step)
        # df_sales_wb_api = detailing.get_wb_sales_realization_api(date_from, date_end, days_step)
        df_sales_wb_api = API_WB.get_wb_stock_api()
        logging.info(time.process_time() - t)
        file = io_output.io_output(df_sales_wb_api)
        logging.info(time.process_time() - t)
        return send_file(file,
                         download_name='report' + str(datetime.date.today()) + str(datetime.time()) + ".xlsx",
                         as_attachment=True)

    return render_template('upload_get_dynamic_sales.html')
