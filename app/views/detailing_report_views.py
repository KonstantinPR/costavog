import app.modules.API_WB
from app import app
import io
import zipfile
from flask import flash, render_template, request, redirect, send_file
from flask_login import login_required, current_user
import pandas as pd
from app.modules import detailing_reports, detailing, API_WB
from app.modules import io_output, yandex_disk_handler, pandas_handler
import numpy as np
from flask import request


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

        is_get_storage = request.form.get('is_get_storage')
        print(f"is_get_storage {is_get_storage}")

        is_just_concatenate = request.form.get('is_just_concatenate')
        print(f"is_just_concatenate {is_just_concatenate}")

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

        concatenated_dfs = pandas_handler.upper_case(concatenated_dfs, 'Артикул поставщика')

        # Check if concatenate parameter is passed
        if is_just_concatenate == 'is_just_concatenate':
            return send_file(io_output.io_output(concatenated_dfs), download_name=f'concat.xlsx', as_attachment=True)

        if 'Хранение' in concatenated_dfs.columns:
            storage_cost = concatenated_dfs['Хранение'].sum()
        else:
            concatenated_dfs['Хранение'] = 0
            storage_cost = 0

        df = detailing.zip_detail(concatenated_dfs, df_net_cost)

        date_min = df["Дата заказа покупателем"].min()
        date_max = df["Дата заказа покупателем"].max()

        is_get_stock = request.form.get('is_get_stock')

        if is_get_stock:
            df_stock = API_WB.get_wb_stock_api_extanded()
            df_stock = pandas_handler.upper_case(df_stock, 'supplierArticle')
            df = df.merge(df_stock, how='outer', left_on='Артикул поставщика', right_on='supplierArticle')

        goods_sum = df['quantity'].sum()
        df['quantity'].replace(np.NaN, 0, inplace=True)
        df['quantity + чист.покупк.'] = df['quantity'] + df['Чист. покупок шт.']
        # df['Хранение'] = df['quantity + чист.покупк.'] * storage_cost / goods_sum
        # df['Маржа-себест.-хран.'] = df['Маржа-себест.'] - df['Хранение']
        # df['Маржа-себест. за шт. руб'] = df['Маржа-себест.-хран.'] / df['Чист. покупок шт.']

        is_get_price = request.form.get('is_get_price')

        if is_get_storage:
            df_storage = API_WB.get_average_storage_cost()
            df_storage = pandas_handler.upper_case(df_storage, 'vendorCode')
            df = df.merge(df_storage, how='outer', left_on='Артикул поставщика', right_on='vendorCode')
            # df['Хранение'] = df['storagePricePerBarcode'] * df['quantity + чист.покупк.']
            df['Хранение'] = storage_cost * df['shareCost']
            df['Хранение'] = df['Хранение'].fillna(0)
            df['Хранение.ед'] = df['Хранение'] / df['quantity + чист.покупк.']
            df['Маржа-себест.-хран.'] = df['Маржа-себест.'] - df['Хранение']
            # Calculate 'Маржа-себест. за шт. руб' based on conditions
            df['Маржа-себест. за шт. руб'] = np.where(
                df['Чист. покупок шт.'] != 0,
                df['Маржа-себест.-хран.'] / df['Чист. покупок шт.'],
                np.where(
                    df['quantity'] != 0,
                    df['Маржа-себест.-хран.'] / df['quantity'],
                    0
                )
            )
            # df.to_excel("detail_with_storage.xlsx")

        nm_columns = ['nmId', 'nm_id']
        df = pandas_handler.fill_empty_val_by(nm_columns, df, "Код номенклатуры")

        if is_get_price:
            df_price = API_WB.get_wb_price_api()
            df = df.merge(df_price, how='outer', left_on='Код номенклатуры', right_on='nm_id')
            df['price_disc'] = df['price'] - df['price'] * df['discount'] / 100
            # df.to_excel("detail_with_price.xlsx")

        df = pandas_handler.fill_empty_val_by(nm_columns, df, "Код номенклатуры")

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
            'Хранение.ед',
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
            'volume',
        ]]

        df.dropna(subset=["Артикул поставщика"], inplace=True)
        file = io_output.io_output(df)

        flash("Отчет успешно выгружен в excel файл")
        return send_file(file, download_name=f'report_detailing_{str(date_max)}_to_{str(date_min)}.xlsx',
                         as_attachment=True)

    return render_template('upload_detailing.html', doc_string=upload_detailing.__doc__)


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
