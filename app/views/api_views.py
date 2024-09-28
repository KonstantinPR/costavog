import logging

import pandas as pd

import app.modules.request_handler
from app import app
from flask import render_template, request, redirect, send_file
from flask_login import login_required, current_user
import datetime
import time
import io
from app.modules import API_WB, detailing_api_module
from app.modules import io_output, yandex_disk_handler, request_handler, pandas_handler


@app.route('/get_sales_funnel_wb', methods=['POST', 'GET'])
@login_required
def get_sales_funnel_wb():
    """
    To get sales_funnel via API WB
    """

    if request.method == 'POST':
        df, file_name = API_WB.get_wb_sales_funnel_api(request)

        return send_file(io_output.io_output(df), download_name=file_name, as_attachment=True)
    return render_template('upload_api_sales_funnel.html', doc_string=get_sales_funnel_wb.__doc__)


@app.route('/get_sales_wb', methods=['POST', 'GET'])
@login_required
def get_sales_wb():
    """
    To get sales via API WB
    """

    if request.method == 'POST':
        date_from = request_handler.request_date_from(request, delta=28)
        date_end = request_handler.request_date_end(request)
        days_step = request_handler.request_days_step(request)
        df_sales = API_WB.get_wb_sales_realization_api_v2(date_from, date_end, days_step)
        df = io_output.io_output(df_sales)
        file_name = f'wb_sales_{str(date_from)}_{str(date_end)}.xlsx'
        return send_file(df, download_name=file_name, as_attachment=True)
    return render_template('upload_sales_wb.html', doc_string=get_sales_wb.__doc__)


@app.route('/get_cards_wb', methods=['POST', 'GET'])
@login_required
def get_cards_wb():
    """
    To get all cards via API WB
    """

    if request.method == 'POST':
        is_to_yadisk = request.form.get('is_to_yadisk')
        is_from_yadisk = request.form.get('is_from_yadisk')
        limit_cards = request.form.get('limit_cards', 0)
        df_all_cards = API_WB.get_all_cards_api_wb(is_from_yadisk=is_from_yadisk, is_to_yadisk=is_to_yadisk,
                                                   limit_cards=limit_cards)
        df = io_output.io_output(df_all_cards)
        file_name = f'wb_api_cards_{str(datetime.datetime.now())}.xlsx'
        return send_file(df, download_name=file_name, as_attachment=True)
    return render_template('upload_cards_wb.html', doc_string=get_cards_wb.__doc__)


@app.route('/get_stock_wb', methods=['POST', 'GET'])
@login_required
def get_stock_wb():
    """
    Достает все остатки с WB через API, на яндекс диск сохранится без городов и размеров
    """
    is_shushary = request.form.get('is_shushary')
    testing_mode = request.form.get('testing_mode')
    logging.warning(f"is_shushary {is_shushary}")
    file_name = f'wb_api_stock_{str(datetime.datetime.now())}.xlsx'
    if request.method == 'POST':
        logging.warning(f"request {request}")
        df = API_WB.get_wb_stock_api(request=request, is_shushary=is_shushary, testing_mode=testing_mode)
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
        logging.warning(f'number_last_days {number_last_days}')
        logging.warning(f"{request.form.get('is_mean')}")

        path_by_config = app.config['YANDEX_KEY_STORAGE_COST']
        yandex_disk_handler.copy_file_to_archive_folder(request=request, path_or_config=path_by_config)

        if request.form.get('is_mean'):
            df_all_cards = API_WB.get_average_storage_cost()
            logging.warning(f"storage cost is received by API WB")
        else:
            df_all_cards = API_WB.get_storage_cost(number_last_days=number_last_days, days_delay=0)

        # logging.warning(f"df {df_all_cards}")
        df = io_output.io_output(df_all_cards)
        file_name = f'storage_data_{str(datetime.datetime.now())}.xlsx'
        return send_file(df, download_name=file_name, as_attachment=True)
    return render_template('upload_storage_wb.html', doc_string=get_storage_wb.__doc__)


# @app.route('/get_wb_sales_realization_api', methods=['POST', 'GET'])
# @login_required
# def get_wb_sales_realization_api():
#     """To get speed of sales for all products in period"""
#     if not current_user.is_authenticated:
#         return redirect('/company_register')
#     if request.method == 'POST':
#         date_from = request_handler.request_date_from(request)
#         date_end = request_handler.request_date_end(request)
#         days_step = request_handler.request_days_step(request)
#         t = time.process_time()
#         logging.warning(time.process_time() - t)
#         # df_sales_wb_api = detailing.get_wb_sales_api(date_from, days_step)
#         # df_sales_wb_api = detailing.get_wb_sales_realization_api(date_from, date_end, days_step)
#         df_sales_wb_api = API_WB.get_wb_sales_realization_api(date_from, date_end, days_step)
#         logging.warning(time.process_time() - t)
#         file = io_output.io_output(df_sales_wb_api)
#         logging.warning(time.process_time() - t)
#         return send_file(file, download_name=f"wb_sales_report-{str(date_from)}-{str(date_end)}-{datetime.time()}.xlsx",
#                          as_attachment=True)
#
#     return render_template('upload_get_dynamic_sales.html')

#
# @app.route('/get_wb_pivot_sells_api', methods=['POST', 'GET'])
# @login_required
# def get_wb_pivot_sells_api() -> object:
#     """
#     Форма возвращает отчет в excel о прибыльности на основе анализа отчетов о продажах, остатков с сайта wb
#     и данных о себестоимости с яндексдиска. Отчет формируется От и до указанных дат - в случае с "динамикой"
#     отчет будет делиться на указанное количество частей, в каждой из которых будет высчитываться прибыль и
#     далее рассчитыватья показатели на основе изменения прибыли от одного периода к другому. На выходе
#     получим сводную таблицу с реккомендациями по скидке.
#     """
#     if not current_user.is_authenticated:
#         return redirect('/company_register')
#     if request.method == 'POST':
#         date_from = request_handler.request_date_from(request)
#         date_end = request_handler.request_date_end(request)
#         days_step = request_handler.request_days_step(request)
#         df = API_WB.get_wb_sales_realization_api(date_from, date_end, days_step)
#         df_sales = detailing_api_module.get_wb_sales_realization_pivot(df)
#         df_stock = API_WB.get_wb_sales_realization_api_v2(date_from=date_from, date_to=date_end)
#         df_net_cost = yandex_disk_handler.get_excel_file_from_ydisk(app.config['NET_COST_PRODUCTS'])
#         df = df_sales.merge(df_stock, how='outer', on='nm_id')
#         df = df.merge(df_net_cost, how='outer', left_on='nm_id', right_on='nm_id')
#         df = detailing_api_module.get_revenue(df)
#         df = detailing_api_module.get_important_columns(df)
#         file = io_output.io_output(df)
#         name_of_file = f"wb_revenue_report-{str(date_from)}-{str(date_end)}-{datetime.time()}.xlsx"
#         return send_file(file, download_name=name_of_file, as_attachment=True)
#
#     return render_template('upload_get_dynamic_sales.html', doc_string=get_wb_pivot_sells_api.__doc__)


@app.route('/get_wb_price_api', methods=['POST', 'GET'])
@login_required
def get_wb_price_api():
    """getting prices amd discounts form api wildberries"""
    if not current_user.is_authenticated:
        return redirect('/company_register')
    if request.method == 'POST':
        df, file_name = API_WB.get_wb_price_api(request)
        print(file_name)
        df = io_output.io_output(df)
        # print(df)
        return send_file(df, download_name=file_name, as_attachment=True)
    return render_template('upload_prices_wb.html', doc_string=get_wb_price_api.__doc__)


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

        logging.warning(date_from)

        if request.form.get('date_end'):
            date_end = request.form.get('date_end')
        else:
            date_end = time.strftime("%Y-%m-%d")

        logging.warning(date_end)

        # if request.form.get('days_step'):
        #     days_step = request.form.get('days_step')
        # else:
        #     days_step = app.config['DAYS_STEP_DEFAULT']

        t = time.process_time()
        logging.warning(time.process_time() - t)
        # df_sales_wb_api = detailing.get_wb_sales_api(date_from, days_step)
        # df_sales_wb_api = detailing.get_wb_sales_realization_api(date_from, date_end, days_step)
        df_sales_wb_api = API_WB.get_wb_stock_api()
        logging.warning(time.process_time() - t)
        file = io_output.io_output(df_sales_wb_api)
        logging.warning(time.process_time() - t)
        return send_file(file,
                         download_name='report' + str(datetime.date.today()) + str(datetime.time()) + ".xlsx",
                         as_attachment=True)

    return render_template('upload_get_dynamic_sales.html')


@app.route('/get_cards_ozon', methods=['POST', 'GET'])
@login_required
def get_cards_ozon():
    client_id = app.config['OZON_CLIENT_ID']
    api_key = app.config['OZON_API_TOKEN']

    if request.method != 'POST':
        return render_template('upload_cards_ozon.html', doc_string=get_cards_ozon.__doc__)

    # Step 1: Create the report
    report_code = API_WB.create_cards_report_ozon(client_id, api_key)
    logging.info(f"Report code: {report_code}")

    if report_code:
        max_retries = 20  # Number of retries
        retry_interval = 20  # Wait seconds between retries

        # Step 2: Retry loop to check for report availability
        for attempt in range(max_retries):
            time.sleep(retry_interval)

            report_info = API_WB.check_report_info_ozon(report_code, client_id, api_key)

            if report_info:
                status = report_info['result']['status']
                print(f"Status attempt {attempt}: {status}")
                if status == 'success':
                    # Report is ready, download the file
                    report_file_url = report_info['result']['file']
                    report_content = API_WB.download_report_file_ozon(report_file_url)

                    if report_content:
                        # Convert CSV content to DataFrame
                        df = pd.read_csv(io.BytesIO(report_content), sep=';')
                        # Ensure the 'Артикул' column is treated as a string
                        columns = ["Артикул", "Barcode"]
                        df = pandas_handler.to_str(df, columns)
                        file_name = f'ozon_cards_report_{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}.xlsx'
                        return send_file(io_output.io_output(df), download_name=file_name, as_attachment=True)

                    logging.error("Error downloading the report file")
                    return "Error downloading the report file", 500
                elif status in ['processing', 'waiting']:
                    continue  # Still processing, retry later
                else:
                    logging.error(f"Report generation failed with status: {status}")
                    return f"Report generation failed with status: {status}", 500
            else:
                logging.error("Error checking report status")
                return "Error checking report status", 500

        return "Report not found or still processing after multiple attempts", 404
    else:
        logging.error("Error creating the report")
        return "Error creating the report", 500


@app.route('/get_stock_ozon', methods=['POST', 'GET'])
@login_required
def get_stock_ozon():
    """
    Get stock report from OZON warehouses via API
    """

    if not request.method == 'POST':
        return render_template('upload_stock_ozon.html', doc_string=get_stock_ozon.__doc__)

    client_id = app.config['OZON_CLIENT_ID']
    api_key = app.config['OZON_API_TOKEN']

    limit = int(request.form.get('limit', 1000))
    offset = int(request.form.get('offset', 0))
    warehouse_type = request.form.get('warehouse_type', 'ALL')

    # Call the helper function to get the stock report
    stock_report = API_WB.get_stock_ozon_api(client_id, api_key, limit, offset, warehouse_type)

    if stock_report:
        columns = [
            'free_to_sell_amount',
            'item_code',
            'item_name',
            'promised_amount',
            'reserved_amount',
            'sku',
            'warehouse_name',
            'idc'
        ]
        df_stock_report = pandas_handler.convert_to_dataframe(stock_report['result']['rows'], columns)
        file_name = f'ozon_stock_report_{str(datetime.datetime.now())}.xlsx'
        return send_file(io_output.io_output(df_stock_report), download_name=file_name, as_attachment=True)
    else:
        return "Error fetching stock report from OZON", 500


@app.route('/get_price_ozon', methods=['POST', 'GET'])
@login_required
def get_price_ozon():
    if request.method != 'POST':
        return render_template('upload_price_ozon.html', doc_string=get_price_ozon.__doc__)

    client_id = app.config['OZON_CLIENT_ID']
    api_key = app.config['OZON_API_TOKEN']
    limit = request.form.get('limit', 1000)

    # Call the function to get prices from OZON API
    prices = API_WB.get_price_ozon_api(limit, client_id=client_id, api_key=api_key)

    # Flattening nested fields such as 'price', 'commissions', 'price_index'
    df = pd.json_normalize(
        prices,
        sep='_',
        record_prefix='',
        meta=['product_id', 'offer_id'],  # Keys to keep as top-level columns
        record_path=None,  # You can specify different paths if deeply nested
        errors='ignore'
    )

    # Generate Excel file from DataFrame
    file = io_output.io_output(df)
    file_name = "prices.xlsx"

    # Send the Excel file to the user
    return send_file(file, as_attachment=True, download_name=file_name)
