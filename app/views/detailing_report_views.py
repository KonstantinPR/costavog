import app.modules.API_WB
import time
import app.modules.API_WB
from app import app
import io
import zipfile
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required, current_user
import datetime
import pandas as pd
from app.modules import detailing_reports, detailing, API_WB
from app.modules import io_output, yandex_disk_handler
import numpy as np


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
    df = detailing_reports.key_indicators_module(file_content)

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
        date_from = detailing_reports.request_date_from(request)
        date_end = detailing_reports.request_date_end(request)
        days_step = detailing_reports.request_days_step(request)
        t = time.process_time()
        print(time.process_time() - t)
        # df_sales_wb_api = detailing.get_wb_sales_api(date_from, days_step)
        # df_sales_wb_api = detailing.get_wb_sales_realization_api(date_from, date_end, days_step)
        df_sales_wb_api = API_WB.get_wb_sales_realization_api(date_from, date_end, days_step)
        print(time.process_time() - t)
        file = io_output.io_output(df_sales_wb_api)
        print(time.process_time() - t)
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
        date_from = detailing_reports.request_date_from(request)
        date_end = detailing_reports.request_date_end(request)
        days_step = detailing_reports.request_days_step(request)
        df = API_WB.get_wb_sales_realization_api(date_from, date_end, days_step)
        df_sales = detailing_reports.get_wb_sales_realization_pivot(df)
        df_stock = API_WB.get_wb_stock_api_extanded()
        df_net_cost = yandex_disk_handler.get_excel_file_from_ydisk(app.config['NET_COST_PRODUCTS'])
        df = df_sales.merge(df_stock, how='outer', on='nm_id')
        df = df.merge(df_net_cost, how='outer', left_on='nm_id', right_on='nm_id')
        df = detailing_reports.get_revenue(df)
        df = detailing_reports.get_important_columns(df)
        file = io_output.io_output(df)
        name_of_file = f"wb_revenue_report-{str(date_from)}-{str(date_end)}-{datetime.time()}.xlsx"
        return send_file(file, download_name=name_of_file, as_attachment=True)

    return render_template('upload_get_dynamic_sales.html', doc_string=get_wb_pivot_sells_api.__doc__)


@app.route('/get_wb_price_api', methods=['POST', 'GET'])
@login_required
def get_wb_price_api():
    if not current_user.is_authenticated:
        return redirect('/company_register')
    df = detailing_reports.get_wb_price_api()
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
        df_sales_wb_api = API_WB.get_wb_stock_api_extanded()
        print(time.process_time() - t)
        file = io_output.io_output(df_sales_wb_api)
        print(time.process_time() - t)
        return send_file(file,
                         download_name='report' + str(datetime.date.today()) + str(datetime.time()) + ".xlsx",
                         as_attachment=True)

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

        df = detailing.concatenate_detailing_modul(uploaded_file, df_net_cost)

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
            df_stock = API_WB.get_wb_stock_api_extanded()
            df = df.merge(df_stock, how='outer', left_on='Артикул поставщика', right_on='supplierArticle')

        file = io_output.io_output(df)

        flash("Отчет успешно выгружен в excel файл")
        return send_file(file, download_name=f"concatenated_detailing_{str(date_start)}_{str(date_end)}.xlsx",
                         as_attachment=True)

    return render_template('upload_detailing.html')


@app.route('/upload_detailing', methods=['POST', 'GET'])
@login_required
def upload_detailing():
    """Processing detailing in excel that can be downloaded in wb portal in zip, put all zip in one zip and upload it,
    or can be chosen some and added to input as well"""

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

        # print(df_net_cost)
        df_list = detailing.zips_to_list(uploaded_file)
        concatenated_dfs = pd.concat(df_list)
        storage_cost = concatenated_dfs['Хранение'].sum()

        if 'Артикул поставщика' in concatenated_dfs:
            concatenated_dfs['Артикул поставщика'] = [str(s).upper() if isinstance(s, str) else s for s in
                                                concatenated_dfs['Артикул поставщика']]

        df = detailing.zip_detail(concatenated_dfs, df_net_cost)

        date_min = df["Дата заказа покупателем"].min()
        date_max = df["Дата заказа покупателем"].max()

        is_get_stock = request.form.get('is_get_stock')

        if is_get_stock:
            df_stock = API_WB.get_wb_stock_api_extanded()
            df = df.merge(df_stock, how='outer', left_on='Артикул поставщика', right_on='supplierArticle')

        goods_sum = df['quantity'].sum()
        df['quantity'].replace(np.NaN, 0, inplace=True)
        df['quantity + чист.покупк.'] = df['quantity'] + df['Чист. покупок шт.']
        df['Хранение'] = df['quantity + чист.покупк.'] * storage_cost / goods_sum
        df['Маржа-себест.-хран.'] = df['Маржа-себест.'] - df['Хранение']
        df['Маржа-себест. за шт. руб'] = df['Маржа-себест.-хран.'] / df['Чист. покупок шт.']

        is_get_price = request.form.get('is_get_price')

        df['Код номенклатуры'] = df['Код номенклатуры'].fillna(df['nm_id'])

        if is_get_price:
            df_price = API_WB.get_wb_price_api()
            df_price.to_excel("wb_price.xlsx")
            df = df.merge(df_price, how='outer', left_on='Код номенклатуры', right_on='nm_id')
            df['price_disc'] = df['price'] - df['price'] * df['discount'] / 100
            df.to_excel("detail_with_price.xlsx")

        df = df[[
            'Бренд',
            'Предмет_x',
            'Артикул поставщика',
            'Код номенклатуры',
            'quantity',
            'quantity + чист.покупк.',
            'price_disc',
            'net_cost',
            'Дней в продаже',
            'Маржа-себест.-хран.',
            'Маржа-себест.',
            'Маржа',
            'Чист. покупок шт.',
            'Продажи',
            'Возвраты, руб.',
            'Продаж',
            'Возврат шт.',
            'Хранение',
            'Услуги по доставке товара покупателю',
            'Покатушка средне, руб.',
            'Маржа-себест. за шт. руб',
            'Покатали раз',
            'company_id',
            'Маржа / логистика',
            'Продажи к возвратам',
            'Маржа / доставковозвратам',
            'Логистика',
            'Доставки/Возвраты, руб.',
            'Себестоимость продаж',
            'Поставщик',
            'Дата заказа покупателем',
            'Дата продажи',
        ]]

        df.dropna(subset=["Артикул поставщика"], inplace=True)
        file = io_output.io_output(df)

        flash("Отчет успешно выгружен в excel файл")
        return send_file(file, download_name=f'report_detailing_{str(date_max)}_to_{str(date_min)}.xlsx',
                         as_attachment=True)

    return render_template('upload_detailing.html', doc_string=upload_detailing.__doc__)
