import app.modules.API_WB
import logging
from app import app
import io
import zipfile
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required, current_user
import pandas as pd
from app.modules import detailing_api_module, detailing_upload_module, API_WB
from app.modules import io_output, yandex_disk_handler, pandas_handler
import numpy as np
from flask import request


@app.route('/key_indicators', methods=['POST', 'GET'])
@login_required
def key_indicators():
    """
    to show key indicators from revenue_tables via YandexDisk file (or revenue_processing route - planning in future)
    1 . market cost of all products on wb
    2 . revenue potential cost of all product on wb (to take medium of revenue if no sells)
    (potential cost by revenue of all products)
    """

    if not current_user.is_authenticated:
        return redirect('/company_register')

    file_content, file_name = yandex_disk_handler.download_from_YandexDisk()
    df = detailing_api_module.key_indicators_module(file_content)

    file_name_key_indicator = f'key_indicator_of_{file_name}'
    file_content = io_output.io_output(df, is_index=True)
    yandex_disk_handler.upload_to_YandexDisk(file_content, file_name_key_indicator)

    file = io_output.io_output(df, is_index=True)

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
        df, file_name = detailing_api_module.revenue_processing_module(request)
        print(file_name)
        file_excel = io_output.io_output(df)
        return send_file(file_excel, download_name=file_name, as_attachment=True)

    return render_template('upload_get_dynamic_sales.html')


@app.route('/concatenate_detailing', methods=['POST', 'GET'])
@login_required
def concatenate_detailing():
    """Processing detailing in excel that can be downloaded in wb portal in zip, put all zip in one zip and upload it"""

    if not current_user.is_authenticated:
        return redirect('/company_register')

    if request.method == 'POST':
        uploaded_files = request.files.getlist("file")
        print(uploaded_files)

        is_net_cost = request.form.get('is_net_cost')
        print(is_net_cost == 'is_net_cost')

        if not uploaded_files:
            flash("Вы ничего не выбрали. Необходим zip архив с zip архивами, скаченными с сайта wb раздела детализаций")
            return render_template('upload_detailing.html')

        if len(uploaded_files) == 1 and uploaded_files[0].filename.endswith('.zip'):
            # If there is only one file and it's a zip file, proceed as usual
            uploaded_file = uploaded_files[0]
        else:
            # If there are multiple files or a single non-zip file, create a zip archive in memory
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
                for file in uploaded_files:
                    zip_file.writestr(file.filename, file.read())

            # Reset the in-memory buffer's position to the beginning
            zip_buffer.seek(0)

            # Set the uploaded_file to the in-memory zip buffer
            uploaded_file = zip_buffer

        if is_net_cost:
            df_net_cost = yandex_disk_handler.get_excel_file_from_ydisk(app.config['NET_COST_PRODUCTS'])
        else:
            df_net_cost = False

        print(df_net_cost)

        df = detailing.concatenate_detailing_module(uploaded_file, df_net_cost)

        def convert_to_date(date_str, default_date_unix=1577836800):  # Default: January 1, 2020
            try:
                return pd.to_datetime(date_str)
            except (TypeError, ValueError):
                return pd.to_datetime(default_date_unix, unit='s')

        # Assuming df is your DataFrame
        df['Дата заказа покупателем'] = df['Дата заказа покупателем'].apply(convert_to_date)

        date_end = df['Дата заказа покупателем'].max()
        df['Дата заказа покупателем'] = df['Дата заказа покупателем'].replace("1900-01-01", date_end)
        date_start = df['Дата заказа покупателем'].min()
        print(date_start)

        is_get_stock = request.form.get('is_get_stock')

        if is_get_stock:
            df_stock = API_WB.get_wb_stock_api_no_sizes()
            df = df.merge(df_stock, how='outer', left_on='Артикул поставщика', right_on='supplierArticle')

        file = io_output.io_output(df)

        flash("Отчет успешно выгружен в excel файл")
        return send_file(file, download_name=f"concatenated_detailing_{str(date_start)}_{str(date_end)}.xlsx",
                         as_attachment=True)

    return render_template('upload_detailing.html')
