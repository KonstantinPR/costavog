import logging
from app import app
from flask import render_template, request, redirect, send_file
from flask_login import login_required, current_user
import datetime
import time
from app.modules import API_WB, detailing_api_module, sales_funnel_module
from app.modules import io_output, yandex_disk_handler, request_handler, pandas_handler


@app.route('/sales_funnel_analyses', methods=['POST', 'GET'])
@login_required
def sales_funnel_analyses():
    """
    Sales_funnel_analyses, Data takes via API or YandexDisk
    """

    if request.method == 'POST':
        testing_mode = request.form.get('testing_mode')

        yandex_disk_handler.copy_file_to_archive_folder(request=request,
                                                        path_or_config=app.config['YANDEX_SALES_FUNNEL_WB'])

        column_first = ['nmID', 'vendorCode', 'delta', 'func_discount', 'discount', 'buyoutsCount',
                        'quantityFull', 'ordersCount',
                        'cancelCount', 'avgPriceRub', 'storagePricePerBarcode',
                        'net_cost', 'price', 'price_disc', 'disc_recommended', 'price_recommended']

        df, file_name = API_WB.get_wb_sales_funnel_api(request, testing_mode=testing_mode)
        df_storage = API_WB.get_average_storage_cost(testing_mode=testing_mode)
        df = pandas_handler.df_merge_drop(df, df_storage, left_on='nmID', right_on='nmId')
        df_net_cost = yandex_disk_handler.get_excel_file_from_ydisk(app.config['NET_COST_PRODUCTS'])
        df = pandas_handler.df_merge_drop(df, df_net_cost, left_on='nmID', right_on='nm_id')
        stock_settings = {'no_city': 'no_city', 'no_sizes': 'no_sizes'}
        df_stock = API_WB.get_wb_stock_api(request=stock_settings, testing_mode=testing_mode, is_shushary=True)
        df_stock = df_stock.drop_duplicates(subset='nmId')
        df = pandas_handler.df_merge_drop(df, df_stock, left_on='nmID', right_on='nmId')
        df_price, _ = API_WB.get_wb_price_api(request, testing_mode=testing_mode)
        df_price = df_price.drop_duplicates(subset='nmID')
        df = pandas_handler.df_merge_drop(df, df_price, left_on='nmID', right_on='nmID')
        df_rating = yandex_disk_handler.get_excel_file_from_ydisk(app.config['RATING'])
        df = pandas_handler.df_merge_drop(df, df_rating, left_on='nmID', right_on='Артикул')
        DISCOUNT_COLUMNS = sales_funnel_module.DISCOUNT_COLUMNS
        df = sales_funnel_module.calculate_discount(df, discount_columns=DISCOUNT_COLUMNS)

        include_column = [col for col in column_first if col in df.columns]
        df = df[include_column + [col for col in df.columns if col not in column_first]]
        df = df.drop_duplicates(subset='vendorCode')

        return send_file(io_output.io_output(df), download_name=file_name, as_attachment=True)
    return render_template('upload_sales_funnel.html', doc_string=sales_funnel_analyses.__doc__)
