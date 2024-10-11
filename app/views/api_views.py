import logging
import pandas as pd
import app.modules.request_handler
from app import app
from flask import render_template, request, redirect, send_file, flash, url_for
from flask_login import login_required, current_user
import datetime
import time
import io
import requests
from app.modules import API_WB, API_OZON
from app.modules import io_output, yandex_disk_handler, request_handler, pandas_handler
from varname import nameof


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
    file_name = f'wb_api_stock_{str(datetime.datetime.now())}.xlsx'
    if request.method == 'POST':
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

    if not request.method == 'POST':
        return render_template('upload_storage_wb.html', doc_string=get_storage_wb.__doc__)

    number_last_days = request_handler.request_last_days(request, input_name='number_last_days')

    if not number_last_days: number_last_days = app.config['LAST_DAYS_DEFAULT']

    path_by_config = app.config['YANDEX_KEY_STORAGE_COST']
    yandex_disk_handler.copy_file_to_archive_folder(request=request, path_or_config=path_by_config)

    if request.form.get('is_mean'):
        df_all_cards = API_WB.get_average_storage_cost()
    else:
        df_all_cards = API_WB.get_storage_cost(number_last_days=number_last_days, days_delay=0)

    df = io_output.io_output(df_all_cards)
    file_name = f'storage_data_{str(datetime.datetime.now())}.xlsx'
    return send_file(df, download_name=file_name, as_attachment=True)


@app.route('/get_wb_price_api', methods=['POST', 'GET'])
@login_required
def get_wb_price_api():
    """getting prices amd discounts form api wildberries"""
    if not current_user.is_authenticated:
        return redirect('/company_register')

    if not request.method == 'POST':
        return render_template('upload_prices_wb.html', doc_string=get_wb_price_api.__doc__)

    df, file_name = API_WB.get_wb_price_api(request)
    print(file_name)
    df = io_output.io_output(df)
    return send_file(df, download_name=file_name, as_attachment=True)


@app.route('/get_wb_stock_api', methods=['POST', 'GET'])
@login_required
def get_wb_stock_api():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    if not request.method == 'POST':
        return render_template('upload_get_dynamic_sales.html')

    df_sales_wb_api = API_WB.get_wb_stock_api()
    file = io_output.io_output(df_sales_wb_api)
    file_name = 'report' + str(datetime.date.today()) + str(datetime.time()) + ".xlsx"
    return send_file(file, download_name=file_name, as_attachment=True)


@app.route('/get_wb_sales_realization', methods=['POST', 'GET'])
@login_required
def get_wb_sales_realization():
    """
    get wb sales realization via API WB
    """

    if not request.method == 'POST':
        return render_template('upload_wb_sales_realization_api.html', doc_string=get_wb_sales_realization.__doc__)

    df = API_WB.get_wb_sales_realization_api_v3(request)
    file = io_output.io_output(df)
    file_name = 'sales_report' + str(datetime.date.today()) + str(datetime.time()) + ".xlsx"
    return send_file(file, download_name=file_name, as_attachment=True)


@app.route('/get_cards_ozon', methods=['POST', 'GET'])
@login_required
def get_cards_ozon():
    client_id = app.config['OZON_CLIENT_ID']
    api_key = app.config['OZON_API_TOKEN']

    if request.method != 'POST':
        return render_template('upload_cards_ozon.html', doc_string=get_cards_ozon.__doc__)

    is_to_yadisk = 'is_to_yadisk' in request.form
    is_from_yadisk = 'is_from_yadisk' in request.form
    testing_mode = 'testing_mode' in request.form

    path = app.config['YANDEX_CARDS_OZON']

    if is_from_yadisk:
        df, _ = yandex_disk_handler.download_from_YandexDisk(path)
    else:
        report_code = API_OZON.create_cards_report_ozon(client_id, api_key)
        print(f"Report code: {report_code}")

        # Process the report, fetching the content after retries
        report_content = API_OZON.process_cards_report(report_code, client_id, api_key, max_retries=20,
                                                       retry_interval=20)

        # Convert CSV content to DataFrame
        df = pandas_handler.csv_to_df(report_content)

    # If DataFrame creation failed, log and return an appropriate error message
    if df is None or df.empty:
        logging.error("Failed to generate the DataFrame from the report content.")
        return "Failed to generate the report or the report is empty. Please try again.", 500

    columns = ['FBO OZON SKU ID', 'Barcode']
    df = pandas_handler.to_str(df, columns=columns)
    df = pandas_handler.replace_false_values(df, columns=columns)

    df = pandas_handler.fill_empty_val_by(nm_columns='Ozon Product ID', df=df, col_name_with_missing='FBO OZON SKU ID')

    # Upload to YandexDisk if requested
    yandex_disk_handler.upload_to_YandexDisk(df, f"cards_ozon.xlsx", path=path, is_to_yadisk=is_to_yadisk)

    # Send the Excel file to the user
    file_name = f'ozon_cards_report_{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}.xlsx'
    return send_file(io_output.io_output(df), download_name=file_name, as_attachment=True)


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
    is_to_yadisk = 'is_to_yadisk' in request.form
    testing_mode = 'testing_mode' in request.form

    df = API_OZON.get_stock_ozon_api(client_id, api_key, limit, offset, warehouse_type, is_to_yadisk=is_to_yadisk,
                                     testing_mode=testing_mode)

    file_name = f'ozon_stock_report_{str(datetime.datetime.now())}.xlsx'

    return send_file(io_output.io_output(df), download_name=file_name, as_attachment=True)


@app.route('/get_price_ozon', methods=['POST', 'GET'])
@login_required
def get_price_ozon():
    if request.method != 'POST':
        return render_template('upload_price_ozon.html', doc_string=get_price_ozon.__doc__)

    client_id = app.config['OZON_CLIENT_ID']
    api_key = app.config['OZON_API_TOKEN']
    limit = request.form.get('limit', 1000)

    is_to_yadisk = 'is_to_yadisk' in request.form
    testing_mode = 'testing_mode' in request.form

    # Call the function to get prices from OZON API
    df = API_OZON.get_price_ozon_api(limit, client_id=client_id, api_key=api_key, normalize=True)

    file_name = f"price_ozon.xlsx"
    if is_to_yadisk:
        yandex_disk_handler.upload_to_YandexDisk(df, file_name, path=app.config['YANDEX_PRICE_OZON'])

    # Generate Excel file from DataFrame
    file = io_output.io_output(df)
    file_name = "prices.xlsx"

    # Send the Excel file to the user
    return send_file(file, as_attachment=True, download_name=file_name)


@app.route('/get_realization_report_ozon', methods=['POST', 'GET'])
@login_required
def get_realization_report_ozon():
    if request.method != 'POST':
        return render_template('upload_get_realization_report_ozon.html', doc_string=get_price_ozon.__doc__)

    client_id = app.config['OZON_CLIENT_ID']
    api_key = app.config['OZON_API_TOKEN']

    # Call the function to get prices from OZON API

    month, year = API_OZON.check_date_realization_report_ozon()
    print(f"get_realization_report_ozon for {month}.{year}")
    json_response = API_OZON.get_realization_report_ozon_api(client_id=client_id, api_key=api_key, month=month,
                                                             year=year)
    # Extract rows from the json_response and convert to DataFrame
    rows = json_response.get('rows', [])
    df = pd.json_normalize(rows)  # Convert rows to a DataFrame

    # Optional: Add header details to the DataFrame if needed
    header = json_response.get('header', {})
    for key, value in header.items():
        df[key] = value

    # Generate Excel file from DataFrame
    file = io_output.io_output(df)
    file_name = f"realization_report_ozon_{month}_{year}.xlsx"

    # Send the Excel file to the user
    return send_file(file, as_attachment=True, download_name=file_name)


@app.route('/get_transaction_list_ozon', methods=['POST', 'GET'])
@login_required
def get_transaction_list_ozon():
    if request.method != 'POST':
        return render_template('upload_get_transaction_list_ozon.html', doc_string=get_transaction_list_ozon.__doc__)

    # Check if the checkbox is selected
    group_by_items = 'group_by_items' in request.form
    is_count = 'is_count' in request.form
    is_merge_cards = 'is_merge_cards' in request.form
    is_merge_stock = 'is_merge_stock' in request.form
    is_merge_price = 'is_merge_price' in request.form
    is_to_yadisk = 'is_to_yadisk' in request.form
    testing_mode = 'testing_mode' in request.form

    client_id = app.config['OZON_CLIENT_ID']
    api_key = app.config['OZON_API_TOKEN']

    # Extract dates from the form
    date_from = request.form.get('date_from')
    date_to = request.form.get('date_to')

    # Set default dates if fields are empty
    if not date_from:
        date_from = (datetime.datetime.utcnow() - datetime.timedelta(days=29)).strftime("%Y-%m-%d")
    if not date_to:
        date_to = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    # Append time components to match the required format (ISO 8601)
    date_from = f"{date_from}T00:00:00.000Z"
    date_to = f"{date_to}T23:59:59.999Z"  # End of the day

    print(f"dates {date_from} and {date_to}")

    # Call the function to get the transaction list from OZON API
    json_response = API_OZON.get_transaction_list_ozon_api(client_id=client_id, api_key=api_key, date_from=date_from,
                                                           date_to=date_to)

    # Extract operations from the json_response
    operations = json_response.get('operations', [])

    # Normalize the main operations to a DataFrame
    df_general = pd.json_normalize(operations)

    # Call the normalization function
    nested_columns = ['items', 'services']  # Add more nested column names as needed
    df_general = API_OZON.flatten_nested_columns(df_general, columns=nested_columns, isNormalize=True)

    # Fill empty items_sku with "Other"
    df_general['items_sku'].fillna("Other", inplace=True)

    # Count positive and negative accruals for each items_sku
    in_col = ["accruals_for_sale"]
    df_by_art_size = API_OZON.count_items_by(df_general, col="items_sku", in_col=in_col, negative=True,
                                             is_count=is_count)

    nonnumerical = ["operation_id"]
    df_by_art_size = API_OZON.aggregate_by(col_name="items_sku", df=df_by_art_size, nonnumerical=nonnumerical,
                                           is_group=group_by_items)

    # Convert specific columns to string if needed
    columns = ["posting.warehouse_id", "items_sku"]
    df_by_art_size = pandas_handler.to_str(df_by_art_size, columns=columns)

    df_by_art_size = df_by_art_size.drop_duplicates(subset="items_sku")

    if is_merge_cards:
        path = app.config['YANDEX_CARDS_OZON']
        card_df, _ = yandex_disk_handler.download_from_YandexDisk(path=path, testing_mode=testing_mode)
        df_by_art_size = pandas_handler.df_merge_drop(left_df=card_df, right_df=df_by_art_size,
                                                      left_on="FBO OZON SKU ID",
                                                      right_on="items_sku",
                                                      how='outer')

    df_by_art_size = pandas_handler.fill_empty_val_by(nm_columns="FBO OZON SKU ID", df=df_by_art_size,
                                                      col_name_with_missing='items_sku')

    if is_merge_stock:
        path = app.config['YANDEX_STOCK_OZON']
        stock_df, _ = yandex_disk_handler.download_from_YandexDisk(path=path)
        stock_df = API_OZON.aggregate_by('sku', df=stock_df)
        df_by_art_size = pandas_handler.df_merge_drop(left_df=df_by_art_size, right_df=stock_df,
                                                      left_on="items_sku",
                                                      right_on="sku",
                                                      how='outer')

    df_by_art_size = df_by_art_size.drop_duplicates(subset="items_sku")
    income_outcome_columns = ['services_price', 'amount', 'sale_commission', 'accruals_for_sale']
    df_by_art_size['income'] = df_by_art_size[income_outcome_columns].sum(axis=1)

    df_by_art = pandas_handler.replace_false_values(df=df_by_art_size, false_list=pandas_handler.NAN_LIST,
                                                    columns='Артикул')

    df_by_art = API_OZON.item_code_without_sizes(df_by_art, art_col_name='Артикул', in_to_col='clear_sku')
    nonnumerical = ['items_sku', 'FBS OZON SKU ID', 'FBO OZON SKU ID', 'Barcode', ]
    df_by_art = API_OZON.aggregate_by(col_name="clear_sku", df=df_by_art, nonnumerical=nonnumerical)

    dfs_dict = {'df_general': df_general,
                'df_art_size': df_by_art_size,
                'df_art': df_by_art,
                }

    # Filter out the empty DataFrames and their names
    filtered_dfs_list, filtered_dfs_names_list = pandas_handler.keys_values_in_list_from_dict(dfs_dict, ext='.xlsx')

    # Now you can call files_to_zip with the filtered lists

    path = app.config['YANDEX_TRANSACTION_OZON']
    yandex_disk_handler.upload_to_YandexDisk(filtered_dfs_list[1], filtered_dfs_names_list[1], path=path,
                                             testing_mode=testing_mode,
                                             is_to_yadisk=is_to_yadisk)

    print(f"ready to zip {filtered_dfs_names_list}")

    file, name = pandas_handler.files_to_zip(filtered_dfs_list, filtered_dfs_names_list)

    # Send the Excel file to the user
    return send_file(file, as_attachment=True, download_name=name)


@app.route('/analyze_transactions_ozon', methods=['POST', 'GET'])
@login_required
def analyze_transactions_ozon():
    if request.method != 'POST':
        return render_template('upload_analyze_transactions_ozon.html', doc_string=analyze_transactions_ozon.__doc__)

    # Get the file from the form
    file = request.files.get('file')

    # Read the uploaded Excel file into a DataFrame
    df = pd.read_excel(file)

    # Aggregate data by Артикул (including handling of empty values)
    df_aggregated = API_OZON.aggregate_by(col_name="Артикул", df=df)

    # Generate Excel output file
    file_output = io_output.io_output(df_aggregated)
    file_name = "analyze_transactions_ozon.xlsx"

    # Send the file to the user as an Excel attachment
    return send_file(file_output, as_attachment=True, download_name=file_name)


@app.route('/update_price_ozon', methods=['GET', 'POST'])
@login_required
def update_price_ozon():
    if not request.method == 'POST':
        return render_template('upload_update_price_ozon.html', doc_string=update_price_ozon.__doc__)

    file = request.files['excel_file']

    client_id = app.config['OZON_CLIENT_ID']
    api_key = app.config['OZON_API_TOKEN']

    if not file:
        flash('No file uploaded.', 'danger')
        return redirect(url_for('update_price_ozon'))

    try:
        # Read the Excel file into a DataFrame
        excel_data = file.read()
        df = pd.read_excel(io.BytesIO(excel_data))

        # Assuming the DataFrame has the necessary columns for the API
        prices_data = [
            {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "min_price": str(row['min_price']),
                "offer_id": str(row['offer_id']),
                "old_price": str(row['old_price']),
                "price": str(row['price']),
                "price_strategy_enabled": "UNKNOWN",
                "product_id": row['product_id'],
            }
            for index, row in df.iterrows()
        ]

        # print(prices_data)

        # Call batch update prices to handle more than 1000 items
        API_OZON.batch_update_prices(prices_data, client_id=client_id, api_key=api_key)

        return redirect(url_for('update_price_ozon'))
    except Exception as e:
        flash(f'Error updating prices: {str(e)}', 'danger')
        return redirect(url_for('update_price_ozon'))
